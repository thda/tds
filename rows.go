package tds

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/thda/tds/netlib"
)

// Rows information, columns and data
type Rows struct {
	s           *session
	row         *row     // rows
	columns     *columns // regular columns
	wideColumns *columns // wide columns
	params      *columns // regular params
	wideParams  *columns // wide columns
	columnFmts  []colFmt // current columns' format
	// computed row data/column format
	cmpRow     *cmpRow
	cmpColumns *cmpColumns
	// for browse information
	tableName   *tableName
	columnsInfo *colInfo
	// map indicating which tokens are stored where when processing response
	messageMap       map[byte]netlib.Messager
	hasNextResultSet bool // true if another resultSet is comming
	hasCmpInfo       bool // true if this is a computed column result set
	isCmpRow         bool // if the returned row is a computed row
	err              error
	ctx              context.Context
}

// rows free list
var rowPool = sync.Pool{
	New: func() interface{} {
		rows := &Rows{row: &row{msg: newMsg(Row)},
			cmpColumns:  &cmpColumns{msg: newMsg(CmpRowFmt)},
			columns:     &columns{msg: newMsg(ColumnFmt)},
			wideColumns: &columns{msg: newMsg(WideColumnFmt), flags: wide},
			tableName:   &tableName{msg: newMsg(TableName)},
			params:      &columns{msg: newMsg(ColumnFmt), flags: param},
			wideParams:  &columns{msg: newMsg(WideColumnFmt), flags: wide | param},
			columnsInfo: &colInfo{msg: newMsg(ColumnInfo)},
			cmpRow:      &cmpRow{msg: newMsg(CmpRow), infos: make(map[uint16]cmpColumns)}}

		// where to store the token...
		rows.messageMap = map[byte]netlib.Messager{
			byte(CmpRow):        rows.cmpRow,
			byte(CmpRowFmt):     rows.cmpColumns,
			byte(Row):           rows.row,
			byte(WideColumnFmt): rows.wideColumns,
			byte(ColumnFmt):     rows.columns,
			// params are handled like rows
			byte(Param):      rows.row,
			byte(ParamFmt2):  rows.wideParams,
			byte(ParamFmt):   rows.params,
			byte(ColumnInfo): rows.columnsInfo,
			byte(TableName):  rows.tableName,
		}
		rows.columnsInfo.tables = rows.tableName
		return rows
	},
}

// return a new result set and fetch the headers
func newRow(ctx context.Context, s *session) (*Rows, error) {
	// get from pool and reset state values
	rows := rowPool.Get().(*Rows)
	rows.s, rows.hasNextResultSet, rows.err = s, false, nil
	rows.ctx = ctx
	rows.columnFmts = nil

	// get the first header info
	rows.err = rows.Next(nil)

	// return errors straigt away
	if rows.err != io.EOF && rows.err != nil {
		return rows, rows.err
	}

	// completely ignore the first EOF
	// when it is the end of column desc messages
	if rows.HasNextResultSet() {
		rows.err = nil
	}
	rows.hasNextResultSet = false

	// First EOF will be returned by the next scan
	return rows, nil
}

// Columns returns the resultset's columns
// Satisfies the driver.Rows interface
func (r Rows) Columns() (columns []string) {
	if r.columnFmts == nil {
		return
	}
	columns = make([]string, len(r.columnFmts))
	for i, column := range r.columnFmts[:] {
		columns[i] = column.name
		if column.realName != "" && column.name == "" {
			columns[i] = column.realName
		}
	}
	return
}

// Close skips all remaining rows
// NB: only return error on unexpected failure.
func (r *Rows) Close() (err error) {
	defer rowPool.Put(r)
	for {
		for {
			err = r.Next(nil)
			if err == io.EOF {
				// we reached EOF, exit without error.
				err = nil
				break
			} else if err != nil {
				return fmt.Errorf("tds: rows.Close failed: %s", err)
			}
		}
		if r.HasNextResultSet() {
			if err = r.NextResultSet(); err != nil {
				return fmt.Errorf("tds: rows.Close failed: %s", err)
			}
		} else {
			break
		}
	}

	return err
}

// HasNextResultSet indicates of there is a second result set pending
func (r Rows) HasNextResultSet() bool {
	return r.hasNextResultSet
}

// NextResultSet resets the hasNextResultSet
// to trigger the processing at the next call to Next()
func (r *Rows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.hasNextResultSet = false
	r.s.clearResult()
	return nil
}

// Next implements the driver.Result Next method to fetch the next row
//
// It will return io.EOF at the end of the result set
// If another resultset is found, sets the hasNextResultSet property to true.
func (r *Rows) Next(dest []driver.Value) (err error) {
	// next resultset expected
	if r.hasNextResultSet {
		return io.EOF
	}

	// row in error, return immediatly
	if r.err != nil {
		return r.s.checkErr(r.err, "tds: rows.Next failed", false)
	}

	// fetched a computed row to return.
	// It was processed by the previous call to Next()
	if r.isCmpRow {
		r.isCmpRow = false
		copy(dest, r.cmpRow.data)

		// see if there is another result set afterwards
		// TODO: check if other types of token can be sent
		// between a cmpRowToken and another rowToken
		next, err := r.s.b.Peek()
		if err != nil && err != io.EOF {
			return fmt.Errorf("tds: fetching computed result failed: %s", err)
		}
		r.hasNextResultSet = token(next) == Row
		r.columnFmts = r.row.columns
		return nil
	}

	tok, err := r.s.processResponse(r.ctx, r.messageMap, true)

	// a done token without doneMoreResults set
	// will cause processResponse to return EOF and quit here
	if err != nil {
		r.err = err
		return err
	}

	switch token(tok) {
	default:
		return fmt.Errorf("tds: unexpected token: %s", token(tok))
	case Param:
		return r.Next(dest)
	case Row:
		copy(dest, r.row.data)
		return nil
	case TableName, ColumnInfo, Done:
		return r.Next(dest)
	case WideColumnFmt, ColumnFmt, ParamFmt, ParamFmt2:
		switch token(tok) {
		case WideColumnFmt:
			r.row.columns = r.wideColumns.fmts
		case ColumnFmt:
			r.row.columns = r.columns.fmts
		// ignore parameters
		case ParamFmt2:
			r.row.columns = r.wideParams.fmts
			return r.Next(dest)
		case ParamFmt:
			r.row.columns = r.params.fmts
			return r.Next(dest)
		}
		r.columnFmts = r.row.columns
		r.columnsInfo.columns = &r.columnFmts
		r.hasCmpInfo = false
		r.hasNextResultSet = true
		return io.EOF
	case CmpRow:
		// indicate that the next row is a compute result set
		r.isCmpRow = true
		r.hasNextResultSet = true
		r.columnFmts = r.cmpColumns.fmts
		return io.EOF
	case CmpRowFmt:
		// computed info found
		r.hasCmpInfo = true

		// build the label from the column info.
		// not optimized, unfrequent operation
		for i, column := range r.cmpColumns.fmts {
			label := cmpLabels[column.cmpOperator] + "("
			if len(r.columnFmts) <= int(column.cmpOperand-1) ||
				r.columnFmts[column.cmpOperand-1].name == "" {
				label += "<unknown>"
			} else {
				label += r.columnFmts[column.cmpOperand-1].name
			}
			label += ")"
			r.cmpColumns.fmts[i].name = label
		}
		r.cmpRow.infos[r.cmpColumns.id] = *r.cmpColumns
		return r.Next(dest)
	}
}

// ComputedColumnInfo returns the operator and the operand
// for a computed column, given its index
//
// This result is valid only after the computed row was returned.
// Indeed, a statement can contain several compute clause.
// Sybase sends compute inforamtion tokens, along with an ID to match the row
// and the relevant columns' information.
// Here we only handle the last computed result received from the wire,
// as those are overriden in the row handling routine.
func (r Rows) ComputedColumnInfo(index int) (operator string, operand int, ok bool) {
	if !r.hasCmpInfo || index > len(r.cmpColumns.fmts) {
		return
	}
	return cmpLabels[r.cmpColumns.fmts[index].cmpOperator],
		int(r.cmpColumns.fmts[index].cmpOperand), true
}

// ComputeByList the list of columns in the "by" clause of a compute
//
// the result is an array containing the indices.
// This result is valid only after the computed row was returned.
// See ComputedColumnInfo() for the reason
func (r Rows) ComputeByList() (list []int, ok bool) {
	if !r.hasCmpInfo {
		return
	}
	return r.cmpColumns.byColumns, true
}

// ReturnStatus returns the last return status for the current resultset.
// Satisfies the driver.Rows interface
func (r Rows) ReturnStatus() (returnStatus int, ok bool) {
	return r.s.res.returnStatus, r.s.res.hasReturnStatus
}

// AffectedRows returns the number of affected rows
// Satisfies the driver.Rows interface
func (r Rows) AffectedRows() (count int, ok bool) {
	return int(r.s.res.affectedRows), r.s.res.hasAffectedRows
}

// ColumnTypeScanType returns the value type to scan into.
// Satisfies the driver.Rows interface
func (r Rows) ColumnTypeScanType(index int) reflect.Type {
	if index > len(r.columnFmts) {
		return nil
	}
	return r.columnFmts[index].colType.scanType()
}

// ColumnTypeDatabaseTypeName returns the sybase type name as a string.
// Satisfies the driver.Rows interface
func (r Rows) ColumnTypeDatabaseTypeName(index int) string {
	if index > len(r.columnFmts) {
		return "UNKNOWN"
	}
	return r.columnFmts[index].colType.databaseTypeName()
}

// ColumnTypeLength returns the length of a column given by its index.
// Satisfies the driver.Rows interface
func (r Rows) ColumnTypeLength(index int) (int64, bool) {
	if index > len(r.columnFmts) {
		return 0, false
	}
	return r.columnFmts[index].colType.length()
}

// ColumnTypeNullable returns the nullability of a column given by its index.
// Satisfies the driver.Rows interface
func (r Rows) ColumnTypeNullable(index int) (bool, bool) {
	if index > len(r.columnFmts) {
		return false, false
	}
	return r.columnFmts[index].flags&uint32(nullable) != 0, true
}

// ColumnTypePrecisionScale returns the precision and scale of a numeric column given by its index.
// Satisfies the driver.Rows interface
func (r Rows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	if index > len(r.columnFmts) {
		return 0, 0, false
	}
	return r.columnFmts[index].colType.precisionScale()
}

// tds specific properties

// ColumnAutoIncrement returns a boolean indicating if the column is auto-incremented.
func (r Rows) ColumnAutoIncrement(index int) (bool, bool) {
	if index > len(r.columnFmts) {
		return false, false
	}
	return r.columnFmts[index].flags&uint32(identity) != 0, true
}

// ColumnHidden returns a boolean indicating if the column is hidden.
// Sybase returns hidden columns when using "for browse"
func (r Rows) ColumnHidden(index int) (bool, bool) {
	if index > len(r.columnFmts) {
		return false, false
	}
	return r.columnFmts[index].flags&uint32(hidden) != 0, true
}

// ColumnKey returns a boolean indicating if the column is in the primary key.
func (r Rows) ColumnKey(index int) (bool, bool) {
	if index > len(r.columnFmts) {
		return false, false
	}
	return r.columnFmts[index].flags&uint32(key) != 0, true
}
