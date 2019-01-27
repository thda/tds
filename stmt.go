package tds

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/thda/tds/netlib"
)

// TODO: batch inserts via Batch/Submit methods.

// Stmt is a prepared statement implementing the driver.Stmt interface
type Stmt struct {
	d          *dynamic
	s          *session
	ID         int64
	row        *row // parameter values
	paramFmts  *columns
	paramToken token
	msgs       []netlib.Messager
	converters []driver.ValueConverter
	ctx        context.Context
	values     []driver.Value
}

var stmtID int64

// pool for IDs, to eventually reuse them
var idPool = sync.Pool{
	New: func() interface{} {
		atomic.AddInt64(&stmtID, 1)
		return &stmtID
	},
}

// newStmt returns a new result set and fetch the headers
func newStmt(ctx context.Context, s *session, query string) (*Stmt, error) {
	st := &Stmt{s: s, row: &row{}, ctx: ctx}
	if !s.valid {
		return st, driver.ErrBadConn
	}

	params := &columns{msg: newMsg(ParamFmt), flags: param}
	wideParams := &columns{msg: newMsg(ParamFmt2), flags: wide | param}
	st.row = &row{msg: newMsg(Param)}

	// get a statement number
	st.ID = *idPool.Get().(*int64)

	st.d = &dynamic{msg: newMsg(Dynamic2), operation: dynamicPrepare,
		name:      "gtds" + fmt.Sprintf("%d", st.ID),
		statement: "create proc gtds" + fmt.Sprintf("%d", st.ID) + " as " + query}

	// send query
	err := s.b.Send(ctx, netlib.Normal, st.d)
	if err = s.checkErr(err, "tds: Prepare failed", false); err != nil {
		return st, err
	}

	st.s.clearResult()
	st.d.statement = ""

	// parse parameters info and return status
	// the server will spew out a rowfmt, but it's safe to ignore it, it will be resent
	_, err = s.processResponse(ctx,
		map[byte]netlib.Messager{byte(Dynamic2): st.d,
			byte(ParamFmt): params, byte(ParamFmt2): wideParams}, false)
	if err = s.checkErr(err, "tds: Prepare failed", true); err != nil {
		return st, err
	}

	// assign the expected parameters' values
	if len(params.fmts) > 0 {
		st.row.columns = params.fmts
		st.paramFmts = params
	}

	if len(wideParams.fmts) > 0 {
		st.row.columns = wideParams.fmts
		st.paramFmts = wideParams
	}

	st.values = make([]driver.Value, len(st.row.columns))

	// now ready to exec
	st.d.operation = dynamicExec

	// this query has parameters
	if st.paramFmts != nil {
		st.d.status |= dynamicHasArgs

		// allocate the array containing the valuers and fetch them
		st.converters = make([]driver.ValueConverter, len(st.paramFmts.fmts))
		for i := 0; i < len(st.paramFmts.fmts); i++ {
			st.converters[i] = st.paramFmts.fmts[i].parameterConverter()
		}

		// cache the messages to send for each exec
		st.msgs = []netlib.Messager{st.d, st.paramFmts, st.row}
	} else {
		st.d.status &^= dynamicHasArgs
		st.msgs = []netlib.Messager{st.d}
	}

	return st, nil
}

// send sends the execute to the server
func (st *Stmt) send(ctx context.Context, args []driver.Value) (err error) {
	if !st.s.valid {
		return driver.ErrBadConn
	}

	if len(args) != len(st.row.columns) {
		return fmt.Errorf("tds: parameter count mismatch, expected %d, got %d",
			len(st.row.columns), len(args))
	}

	st.row.data = args
	err = st.s.b.Send(ctx, netlib.Normal, st.msgs[:]...)
	st.s.clearResult()

	return err
}

// Exec executes a prepared statement.
// Implements the database/sql/Stmt interface
func (st *Stmt) Exec(args []driver.Value) (res driver.Result, err error) {
	// send the parameters and the dynamic token
	if err = st.send(st.ctx, args[:]); err != nil {
		return &emptyResult, st.s.checkErr(err, "tds: send failed while execing", false)
	}

	// process the server response
	rows, err := newRow(st.ctx, st.s)

	// discards any row
	if err == nil {
		err = rows.Close()
	}

	return &(*st.s.res), st.s.checkErr(err, "tds: Exec failed", true)
}

// ExecContext executes a prepared statement, along with a context.
// Implements the database/sql/Stmt interface
func (st *Stmt) ExecContext(ctx context.Context, namedArgs []driver.NamedValue) (res driver.Result, err error) {
	if len(namedArgs) != len(st.values) {
		return &emptyResult, fmt.Errorf("tds: ExecContext, invalid arg count")
	}
	for i := 0; i < len(namedArgs); i++ {
		st.values[i] = namedArgs[i].Value
	}

	// send the parameters and the dynamic token
	if err = st.send(ctx, st.values[:]); err != nil {
		return &emptyResult, st.s.checkErr(err, "tds: send failed while execing", false)
	}

	// process the server response
	rows, err := newRow(ctx, st.s)

	// discards any row
	if err == nil {
		err = rows.Close()
	}

	return &(*st.s.res), st.s.checkErr(err, "tds: ExecContext failed", true)
}

// Query executes a prepared statement and returns rows.
func (st *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	// send the parameters and the dynamic token
	if err := st.send(st.ctx, args); err != nil {
		return &emptyRows, st.s.checkErr(err, "tds: send failed while querying", false)
	}

	rows, err := newRow(st.ctx, st.s)
	return rows, st.s.checkErr(err, "tds: QueryContext failed", true)
}

// QueryContext executes a prepared statement and returns rows
func (st *Stmt) QueryContext(ctx context.Context, namedArgs []driver.NamedValue) (driver.Rows, error) {
	args := make([]driver.Value, len(namedArgs))
	for i, nv := range namedArgs {
		args[i] = nv.Value
	}

	// send the parameters and the dynamic token
	if err := st.send(ctx, args); err != nil {
		return &emptyRows, st.s.checkErr(err, "tds: send failed while querying", false)
	}

	rows, err := newRow(ctx, st.s)
	return rows, st.s.checkErr(err, "tds: QueryContext failed", true)
}

// NumInput returns the number of expected parameters
func (st Stmt) NumInput() int {
	return len(st.row.columns)
}

// Close drops the prepared statement from the database
func (st *Stmt) Close() error {
	defer idPool.Put(&st.ID)
	st.d.operation = dynamicDealloc
	st.d.status = 0

	// send message
	err := st.s.b.Send(st.ctx, netlib.Normal, st.d)
	if err = st.s.checkErr(err, "tds: Close failed", false); err != nil {
		return err
	}

	// get response
	// TODO: parse dynamic token to get status
	_, err = st.s.processResponse(st.ctx, nil, false)
	return st.s.checkErr(err, "tds: Prepare failed", true)
}

// ColumnConverter returns converters which check min, max, nullability,
// precision, scale and then convert to a valid sql.Driver value.
func (st Stmt) ColumnConverter(idx int) driver.ValueConverter {
	if idx <= len(st.converters) {
		return st.converters[idx]
	}
	return driver.DefaultParameterConverter
}
