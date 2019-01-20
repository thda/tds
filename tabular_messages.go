package tds

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"

	"github.com/thda/tds/binary"

	"errors"
)

//go:generate stringer -type=columnFlag
type columnFlag int

const (
	hidden   columnFlag = 0x01
	key      columnFlag = 0x02
	writable columnFlag = 0x10
	nullable columnFlag = 0x20
	identity columnFlag = 0x40
)

const (
	wide  = 0x01
	param = 0x02
)

// data type info
type colType struct {
	msg
	userType   int32
	dataType   dataType
	size       int
	precision  int8
	scale      int8
	locale     string
	table      string
	classID    string
	bufferSize uint32
	options    uint8
	// when writing a datatype, we need to get its properties
	// For example int datatypes, in prepared statement parameters,
	// are always sent as nullable.
	// We use encodingProps.encodingType to get the data type to send on the wire
	// We then check for its properties in typeAttributes and store them here.
	writeOptions  uint8
	encodingProps typeAttribute
	valid         bool // indicates if the type was properly mapped
}

// getType returns a typeinfo struct with the proper ser/deser funcs bound to it.
func getType(dataType dataType, size int) colType {
	ti := colType{dataType: dataType, size: size}
	// try to get type props if exists
	ti.getTypeProperties()
	return ti
}

// get the real encoding type if any, and get all the decoders/binary.binary.Encoders.
//
// If the userType indicates a specific underlying type, we use its
// information to get the serializing functions.
// For example, nullable ints, whatever their types, have an IntN dataType.
// We use the usertype to get the concrete type and fetch it in the typeProperties table.
func (t *colType) getTypeProperties() error {
	// get the dataType properties
	props, ok := typeAttributes[t.dataType]
	if !ok {
		return fmt.Errorf("tds: could not find type info for %#x type", byte(t.dataType))
	}
	t.encodingProps = props
	t.valid = true

	// save the type options for reading
	t.options = props.options

	// check if the user type indicates a specific concrete type,
	//  get its properties to merge them.
	if int(t.userType) <= len(concreteTypes) {
		if cProps, ok := typeAttributes[concreteTypes[t.userType]]; ok {
			if props.options&isConcrete == 0 {
				t.encodingProps = cProps
			}
			t.encodingProps.name = cProps.name
		}
	}

	// get the options used to write this datatype
	cProps, ok := typeAttributes[t.encodingProps.encodingType]
	if !ok {
		return nil
	}
	t.writeOptions = cProps.options

	// check the options
	return nil
}

// write serializes the column type infos
// won't return an error, please check the *binary.binary.Encoder's status afterwards
func (t colType) write(e *binary.Encoder) (err error) {
	if !t.valid {
		return errors.New("typeInfo.write: missing type properties")
	}
	e.WriteInt32(t.userType)
	e.WriteByte(byte(t.encodingProps.encodingType))

	if t.options&isLong != 0 {
		if t.size != 0 {
			e.WriteUint32(uint32(t.size))
		} else {
			e.WriteUint32(uint32(t.encodingProps.numBytes))
		}
	} else {
		if t.size != 0 {
			e.WriteUint8(uint8(t.size))
		} else {
			e.WriteInt8(t.encodingProps.numBytes)
		}
	}

	if t.options&isLob != 0 {
		e.WriteStringWithLen(16, t.table)
	}
	if t.options&hasPrec != 0 {
		e.WriteInt8(t.precision)
	}
	if t.options&hasScale != 0 {
		e.WriteInt8(t.scale)
	}
	// locale length
	e.WriteInt8(0)

	return err
}

// read reads the type info
func (t *colType) read(e *binary.Encoder) (err error) {
	t.userType = e.Int32()
	t.dataType = dataType(e.ReadByte())

	// get the type decoding properties
	if err = t.getTypeProperties(); err != nil {
		return fmt.Errorf("tds: could not get type info:%s", err)
	}

	// read size
	if t.options&hasLength != 0 || t.options&isNullable != 0 {
		if t.options&isLong != 0 {
			t.size = int(e.Uint32())
		} else {
			t.size = int(e.Uint8())
		}
	}

	// text fields have table name
	if t.options&isLob != 0 {
		t.table, _ = e.ReadString(16)
	}

	if t.options&hasPrec != 0 {
		t.precision = e.Int8()
	}
	if t.options&hasScale != 0 {
		t.scale = e.Int8()
	}

	// inform the converter of precision and scale
	t.locale, _ = e.ReadString(8)
	return err
}

// Write the data given as an interface to the *binary.Encoder
func (t colType) dataWrite(e *binary.Encoder, data interface{}) error {
	if !t.valid {
		return errors.New("typeInfo.dataWrite: missing type properties")
	}

	// 0 length for null value
	if data == nil {
		if t.writeOptions&isLong != 0 && t.writeOptions&isLob == 0 {
			e.WriteUint32(0)
		} else {
			e.WriteUint8(0)
		}
		return nil
	}

	// write data length for nullable fixed length types
	if t.encodingProps.numBytes != 0 && t.writeOptions&isNullable != 0 {
		e.WriteInt8(t.encodingProps.numBytes)
	}

	return t.encodingProps.writer(e, data, t)
}

// Read the data given as an interface from the encode
func (t *colType) dataRead(e *binary.Encoder) (interface{}, error) {
	if !t.valid {
		return nil, errors.New("tds: missing type properties")
	}

	// read variable length if any
	if t.options&hasLength != 0 || t.options&isNullable != 0 {
		// lobs which have specific 8 bit pointer size to get here
		if t.options&isLong != 0 && t.options&isLob == 0 {
			t.bufferSize = e.Uint32()
		} else {
			t.bufferSize = uint32(e.Uint8())
		}

		if err := e.Err(); err != nil {
			return nil, fmt.Errorf("tds: error while getting type length: %s", err)
		}

		// null value
		if t.bufferSize == 0 {
			return nil, nil
		}
	}
	data, err := t.encodingProps.reader(e, *t)
	return data, err
}

// get the native golang type
func (t colType) scanType() reflect.Type {
	if !t.valid {
		return nil
	}
	return t.encodingProps.scanType
}

// get the database type name
func (t colType) databaseTypeName() string {
	if !t.valid {
		return ""
	}
	return t.encodingProps.name
}

// type length if any
func (t colType) length() (int64, bool) {
	if !t.valid ||
		t.options&hasPrec != 0 || t.options&hasScale != 0 {
		return 0, false
	}

	// long types
	if t.options&isLong != 0 {
		if t.dataType == LongChar {
			return int64(t.size / 2), true
		}
		if t.encodingProps.scanType == reflect.TypeOf("") {
			return int64(t.size), true
		}
		return math.MaxInt32, true
	}

	return int64(t.size), t.options&hasLength != 0
}

// precision & scale
func (t colType) precisionScale() (int64, int64, bool) {
	if !t.valid {
		return 0, 0, false
	}
	if t.options&hasPrec != 0 &&
		t.options&hasScale != 0 {
		return int64(t.precision), int64(t.scale), true
	}
	return 0, 0, false
}

// Print the type information
func (t colType) String() string {
	out := fmt.Sprintf("user type: %3d (%#x)", t.userType, int(t.userType))
	out += fmt.Sprintf("\ndata type: %3d (%#x) %s", t.dataType, int(t.dataType), t.dataType)
	if !t.valid {
		return out + "\ncould not get encoding properties"
	}

	if t.options&hasLength != 0 || t.options&isNullable != 0 {
		out += "\nsize: " + strconv.Itoa(int(t.size))
	}
	if t.options&isLob != 0 {
		out += "\ntable name: " + t.table
	}
	if t.options&hasPrec != 0 {
		out += "\nprecision: " + strconv.Itoa(int(t.precision))
	}
	if t.options&hasScale != 0 {
		out += "\nscale: " + strconv.Itoa(int(t.scale))
	}
	return out
}

// colFmt is the column description
type colFmt struct {
	msg
	name string
	// flags
	flags uint32
	// embed type information
	colType
	// wide column info
	// DB is also given with "for browse" queries
	realName string
	DB       string
	owner    string
	classID  string
	// computed results
	cmpOperator uint8 // max, sum, etc
	cmpOperand  uint8 // column number of the operand
}

// write serializes the column type infos
func (f colFmt) Write(e *binary.Encoder, flags int) error {
	e.WriteStringWithLen(8, f.name)
	if flags&wide != 0 && flags&param == 0 {
		e.WriteStringWithLen(8, f.DB)
		e.WriteStringWithLen(8, f.owner)
		e.WriteStringWithLen(8, f.table)
	}

	if flags&wide != 0 {
		e.WriteUint32(uint32(f.flags))
	} else {
		e.WriteUint8(uint8(f.flags))
	}

	if err := f.colType.write(e); err != nil {
		return err
	}
	// Class for long TDS blobs
	if f.dataType == ExtendedType {
		e.WriteStringWithLen(16, f.classID)
	}
	err := e.Err()
	return err
}

// read reads the columns info
// To write wide column info, give "true" to the wide parameter
func (f *colFmt) Read(e *binary.Encoder, flags int) error {
	f.name, _ = e.ReadString(8)
	if flags&wide != 0 && flags&param == 0 {
		f.DB, _ = e.ReadString(8)
		f.owner, _ = e.ReadString(8)
		f.table, _ = e.ReadString(8)
		f.realName, _ = e.ReadString(8)
	}

	if flags&wide != 0 {
		f.flags = e.Uint32()
	} else {
		f.flags = uint32(e.Uint8())
	}

	if err := f.colType.read(e); err != nil {
		return err
	}

	// Class for long TDS blobs
	if f.dataType == ExtendedType {
		f.classID, _ = e.ReadString(16)
	}

	err := e.Err()
	return err
}

// String implements the stringer interface for a column info
func (f colFmt) String() string {
	out := "name: " + f.name

	if f.DB != "" {
		out += "\ndb: " + f.DB
	}
	if f.owner != "" {
		out += "\nowner: " + f.owner
	}
	if f.table != "" {
		out += "\ntable: " + f.table
	}
	if f.realName != "" {
		out += "\nrealName: " + f.realName
	}
	out += "\n" + f.colType.String()

	flags := ""
	for _, flag := range [...]columnFlag{hidden, key, writable, nullable, identity} {
		if f.flags&uint32(flag) != 0 {
			if flags == "" {
				flags = fmt.Sprint(flag)
			} else {
				flags += "," + fmt.Sprint(flag)
			}
		}
	}
	if flags == "" {
		flags = "none"
	}
	out += "\nflags: " + fmt.Sprintf("%#x", byte(f.flags)) + " (" + flags + ")"
	return out
}

// column converter
func (f *colFmt) parameterConverter() driver.ValueConverter {
	if !f.valid || f.encodingProps.converter == nil {
		return driver.DefaultParameterConverter
	}
	return f.encodingProps.converter(f).(driver.ValueConverter)
}

// columns is an array of column types written to the wire before the actual row data
type columns struct {
	msg
	flags int
	fmts  []colFmt
}

// Write serializes the result headers
func (c columns) Write(e *binary.Encoder) (err error) {
	var colErr error
	if c.flags&wide != 0 && c.flags&param == 0 {
		e.WriteUint32(uint32(len(c.fmts)))
	} else {
		e.WriteUint16(uint16(len(c.fmts)))
	}

	for i := range c.fmts[:] {
		// write columns
		if colErr = c.fmts[i].Write(e, c.flags); colErr != nil {
			err = colErr
		}
	}

	if colErr = e.Err(); colErr != nil {
		return colErr
	}
	return err
}

// Read reads all the wide column information
func (c *columns) Read(e *binary.Encoder) (err error) {
	var colErr error
	colCnt := int(e.Int16())

	c.fmts = make([]colFmt, colCnt)
	for i := range c.fmts[:] {
		// read columns
		if colErr = c.fmts[i].Read(e, c.flags); colErr != nil {
			err = colErr
		}
	}

	if colErr = e.Err(); colErr != nil {
		return colErr
	}
	return err
}

// String implements the stringer interface
func (c columns) String() (out string) {
	for i := range c.fmts[:] {
		out += "Column " + strconv.Itoa(i) + ":\n" + c.fmts[i].String() + "\n"
	}
	return out
}

// row is the actual row data, along with the read/write methods
type row struct {
	msg
	columns []colFmt
	data    []driver.Value
}

// Write serializes the row
func (r row) Write(e *binary.Encoder) (err error) {
	var rowErr error
	if len(r.data) != len(r.columns) {
		return errors.New("row.write: mismatch between count in row and column info")
	}
	for i, t := range r.columns[:] {
		if rowErr = t.dataWrite(e, r.data[i]); rowErr != nil {
			err = rowErr
		}
	}
	if rowErr = e.Err(); rowErr != nil {
		return rowErr
	}
	return err
}

// Read reads one row
func (r *row) Read(e *binary.Encoder) (err error) {
	var rowErr error
	if r.columns != nil && len(r.columns) <= 0 {
		return errors.New("row.read: no type info for row or count mismatch")
	}

	if len(r.data) != len(r.columns) {
		r.data = make([]driver.Value, len(r.columns))
	}

	for i, t := range r.columns[:] {
		r.data[i], rowErr = t.dataRead(e)
		if rowErr != nil {
			err = rowErr
		}
	}
	if rowErr = e.Err(); rowErr != nil {
		return rowErr
	}
	return err
}

// compute operators
const (
	cntToken = 0x4B + iota
	cntUToken
	sumToken
	sumUToken
	avgToken
	avgUToken
	minToken
	maxToken
	countBigToken = 0x61
)

// compute operations labels
var cmpLabels = map[byte]string{
	cntToken:      "count",
	cntUToken:     "count",
	sumToken:      "sum",
	sumUToken:     "sum",
	avgToken:      "avg",
	avgUToken:     "avg",
	minToken:      "min",
	maxToken:      "max",
	countBigToken: "count_big",
}

// description of a compute column
type cmpColumnFmt colFmt

// read reads the columns info
// To write wide column info, give "true" to the wide parameter
func (f *cmpColumnFmt) Read(e *binary.Encoder) error {
	f.cmpOperator = e.Uint8()
	f.cmpOperand = e.Uint8()
	if err := f.colType.read(e); err != nil {
		return err
	}
	err := e.Err()
	return err
}

// list of computed columns
type cmpColumns struct {
	msg
	// id of the compute clause, each statement can have multiple ones
	id uint16
	// array containing the columns numbers of the "by" clauses of the compute
	byColumns []int
	// type info for each computed column
	fmts []colFmt
	// name of the compute clause
	name string
}

// read reads all the computed column informations
func (c *cmpColumns) Read(e *binary.Encoder) error {
	c.id = e.Uint16()
	colCnt := e.Uint8()
	c.fmts = make([]colFmt, colCnt)
	for i := range c.fmts[:] {
		// read columns
		cmpColumnFmt := cmpColumnFmt{msg: c.msg}
		cmpColumnFmt.Read(e)
		c.fmts[i] = colFmt(cmpColumnFmt)
	}

	// number of the columns in the by clause
	byColCnt := e.Uint8()
	c.byColumns = make([]int, byColCnt)
	for i := uint8(0); i < byColCnt; i++ {
		c.byColumns[i] = int(e.Uint8())
	}
	err := e.Err()
	return err
}

// computed row
type cmpRow struct {
	msg
	// the linked compute clause id
	id uint16
	// we need to have all the information on all the computed result sets
	// when a computed row is received
	// it contains the id of the associated row format
	infos map[uint16]cmpColumns
	data  []driver.Value
}

// read reads one computed row
func (r *cmpRow) Read(e *binary.Encoder) error {
	r.id = e.Uint16()
	var err error
	var cols cmpColumns
	var ok bool

	// find the computed columns' information
	if cols, ok = r.infos[r.id]; !ok {
		return errors.New("could not get info for computed row")
	}
	if len(cols.fmts) <= 0 {
		return errors.New("no type info for row or count mismatch")
	}
	if len(r.data) != len(cols.fmts) {
		r.data = make([]driver.Value, len(cols.fmts))
	}
	// read all the rows
	for i, t := range cols.fmts[:] {
		r.data[i], err = t.dataRead(e)
	}
	return err
}

// There for reference, I have yet to see a case where it's filled

// computed row
type cmpName struct {
	msg
	// the linked compute clause id
	computeID uint16
	// we need to have all the information on all the computed result sets
	// when a computed row is received
	// it contains the id of the associated row format
	infos map[uint16]cmpColumns
}

// read reads one computed row
func (r *cmpName) Read(e *binary.Encoder) error {
	r.computeID = e.Uint16()
	var cols cmpColumns
	var ok bool

	// find the computed columns' information
	if cols, ok = r.infos[r.computeID]; !ok {
		return errors.New("could not get info for computed row")
	}
	cols.name, _ = e.ReadString(8)
	r.infos[r.computeID] = cols
	err := e.Err()
	return err
}

const (
	hiddenColumnInfo   = 0x10
	keyColumnInfo      = 0x8
	writableColumnInfo = 0x4
)

// tabName token
type tableName struct {
	msg
	names []string
}

// read reads a tableName struct from the wire
func (t *tableName) Read(e *binary.Encoder) (err error) {
	var name string
	// the underlying reader was set to a limited reader by buffer.readMessage
	for {
		name, _ = e.ReadString(8)
		err = e.Err()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			break
		}
		t.names = append(t.names, name)
	}
	return err
}

// columnInfoToken
type colInfo struct {
	msg
	columns *[]colFmt
	tables  *tableName
}

// read reads a colInfo struct from the wire, and set the relevant options
// as we are given pointers to column list and table names list
func (i *colInfo) Read(e *binary.Encoder) (err error) {
	var columnID, tableID, status uint8
	var name string
	// the underlying reader was set to a limited reader by buffer.readMessage
	for {
		columnID = e.Uint8() - 1
		tableID = e.Uint8() - 1
		status = e.Uint8()
		if status&0x20 != 0 {
			name, _ = e.ReadString(8)
		}
		err = e.Err()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			break
		}
		// we are not at end of stream, let's go

		// no info, we still try to continue to EOF
		if int(columnID)+1 >= len(*i.columns) || int(tableID)+1 >= len(i.tables.names) {
			continue
		}
		(*i.columns)[columnID].table = i.tables.names[tableID]
		(*i.columns)[columnID].realName = name

		// transcode as the flags given in here do not
		// match those in fmt token
		if status&hiddenColumnInfo != 0 {
			(*i.columns)[columnID].flags |= uint32(hidden)
		}
		if status&keyColumnInfo != 0 {
			(*i.columns)[columnID].flags |= uint32(key)
		}
		if status&writableColumnInfo != 0 {
			(*i.columns)[columnID].flags |= uint32(writable)
		}
	}
	return err
}
