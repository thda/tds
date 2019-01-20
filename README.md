

# tds
`import "github.com/thda/tds"`

* [Overview](#pkg-overview)
* [Index](#pkg-index)
* [Subdirectories](#pkg-subdirectories)

## <a name="pkg-overview">Overview</a>
Package tds is a pure Go Sybase ASE/IQ/RS driver for the database/sql package.

### Status
This is a beta release. This driver has yet to be battle tested on
production workload. Version 1.0 will be released
when this driver will be production ready

### Requirements
* Sybase ASE 12.5 or higher
* go 1.8 or higher.

### Installation
Package installation is done via go-get:


	$ go get -u github.com/thda/tds

### Usage
It implements most of the database/sql functionalities.
To connect to a sybase instance, import the package and
use the regular database/sql APIs:


	import (
		"database/sql"
	
		_ "github.com/thda/tds"
	)
	
	func main() {
		cnxStr := "tds://my_user:my_password@dbhost.com:5000/pubs?charset=utf8
		db, err := sql.Open("tds", connStr)
		if err != nil {
			log.Fatal(err)
		}
	
		id := 2
		rows, err := db.Query("select * from authors where id = ?", id)
		…
	}

### Connection String
The connection string is pretty standard and uses the URL format:


	tds://username:password@host:port/database?parameter=value&parameter2=value2

### Connection parameters
The most common ones are:

* username - the database server login. Mandatory.
* password - The login's password. Mandatory.
* host - The host to connect to. Mandatory.
* port - The port to bind to. Mandatory.
* database - The database to use. You will connect to the login's
default database if not specified.
* charset - The client's character set. Default to utf8.
Please refer to the character sets section.
* readTimeout - read timeout in seconds.
* writeTimeout - write timeout in seconds.
* textSize - max size of textsize fields in bytes.
It is suggested to raise it to avoid truncation.

Less frequently used ones:

* ssl - Whether or not to use SSL. THe default is not to use ssl.
Set to "on" if the server is setup to use ssl.
* encryptPassword - Can be "yes" to require password encryption,
"no" to disable it, and "try" to try encrytping password an falling back
to plain text password. Password encryption works on Sybase ASE 15.0.1
or higher and uses RSA.
* packetSize - Network packet size. Must be less than or equal the server's
max network packet size. The default is the server's default network
packet size.
* applicationName - the name of your application.
It is a best practice to set it.

### Query parameters
Most of the database/sql APIs are implemented, with a major one missing:
named parameters. Please use the question mark '?' as a placeholder
for parameters :


	res, err = tx.Exec("insert into author (id, name) values (?, ?)", 2, "Paul")

### Supported data types
Almost all of the sybase ASE datatypes are supported,
with the exception of lob locators.
The type mapping between the server and the go data types is as follows:

* varchar/text/char/unichar/univarchar/xml => string
* int/smalling/bigint => int64
Unsigned bigints with a value > Math.MaxInt64 will be returned as uint64
* date/datetime/bigdate/bigdatetime => time.Time
* image/binary/varbinary/image => []byte
* real/float => float64
* decimal/numeric/money/smallmoney => tds.Num.
Please see the  "precise numerical types" section.

### Precise numerical types
decimal/numeric/money/smallmoney data can be given as parameters using any
of the go numerical types. However one should never use float64
if a loss of precision is not tolerated. To implement precise floating point
numbers, this driver provides a "Num" datatype, which is a wrapper around big.Rat.

It implements the value.Scanner interface, you can thus instanciate it this way:


	num := tds.Num{precision: 16, scale: 2}
	num.Scan("-10.4")
	num.Scan(1023)

To access the underlying big.Rat:


	rat := num.Rat()

Num also implements the stringer interface to pretty print its value.
Please refer to the tds.Num godoc for more information.

### Character set encoding
This driver assumes by default that the client uses utf8 strings and will
ask the server to convert back and forth to/from this charset.

If utf8 charset conversion is not supported on the server, and if the
charset conversion is not supported by golang.org/x/text/encoding,
client-side character set conversion will not be possible.

You will have to handle it yourself and use a charset supported by the server.

### Custom error handling
One can set a custom error callback to process server errors before
the regular error processing routing.
This allows handling showplan messages and print statement.
You must cast your database/sql connection as sql.Conn to use this extension.

The following demonstrates how to handle showplan and print messages:


	conn = goConn.(*tds.Conn)
	
	// print showplan messages and all
	conn.SetErrorhandler(func(m tds.SybError) bool {
		if m.Severity == 10 {
			if (m.MsgNumber >= 3612 && m.MsgNumber <= 3615) ||
				(m.MsgNumber >= 6201 && m.MsgNumber <= 6299) ||
				(m.MsgNumber >= 10201 && m.MsgNumber <= 10299) {
				fmt.Printf(m.Message)
			} else {
				fmt.Println(strings.TrimRight(m.Message, "\n"))
			}
		}
	
		if m.Severity > 10 {
			fmt.Print(m)
		}
		return m.Severity > 10
	})

### Testing
You can use stmt_test.go and session_test.go for sample usage, as follows:


	export TDS_USERNAME=test_user
	export TDS_PASSWORD=test_password
	export TDS_SERVER=localhost:5000
	go test

### License
This driver is release under the go license

### Credits
* the freetds and jtds protocol documentation.
* Microsoft for releasing the full tds specification.
There are differences, however a lot of it is relevant.
* github.com/denisenkom/go-mssqldb for most of the tests.
* The Sybase::TdsServer perl module for capabilities handling.




## <a name="pkg-index">Index</a>
* [Variables](#pkg-variables)
* [type Conn](#Conn)
  * [func NewConn(dsn string) (*Conn, error)](#NewConn)
  * [func (s Conn) Begin() (driver.Tx, error)](#Conn.Begin)
  * [func (s Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error)](#Conn.BeginTx)
  * [func (s Conn) Close() error](#Conn.Close)
  * [func (s Conn) Commit() error](#Conn.Commit)
  * [func (s Conn) Exec(query string, args []driver.Value) (driver.Result, error)](#Conn.Exec)
  * [func (s Conn) ExecContext(ctx context.Context, query string, namedArgs []driver.NamedValue) (driver.Result, error)](#Conn.ExecContext)
  * [func (c Conn) GetEnv() map[string]string](#Conn.GetEnv)
  * [func (s Conn) Ping(ctx context.Context) error](#Conn.Ping)
  * [func (s Conn) Prepare(query string) (driver.Stmt, error)](#Conn.Prepare)
  * [func (s Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error)](#Conn.PrepareContext)
  * [func (s Conn) Query(query string, args []driver.Value) (driver.Rows, error)](#Conn.Query)
  * [func (s Conn) QueryContext(ctx context.Context, query string, namedArgs []driver.NamedValue) (driver.Rows, error)](#Conn.QueryContext)
  * [func (s Conn) Rollback() error](#Conn.Rollback)
  * [func (s Conn) SelectValue(ctx context.Context, query string) (value interface{}, err error)](#Conn.SelectValue)
  * [func (c *Conn) SetErrorhandler(fn func(s SybError) bool)](#Conn.SetErrorhandler)
* [type Errorhandler](#Errorhandler)
* [type Num](#Num)
  * [func (n Num) Rat() big.Rat](#Num.Rat)
  * [func (n *Num) Scan(src interface{}) error](#Num.Scan)
  * [func (n Num) String() string](#Num.String)
* [type Result](#Result)
  * [func (r *Result) LastInsertId() (int64, error)](#Result.LastInsertId)
  * [func (r Result) RowsAffected() (int64, error)](#Result.RowsAffected)
* [type Rows](#Rows)
  * [func (r Rows) AffectedRows() (count int, ok bool)](#Rows.AffectedRows)
  * [func (r *Rows) Close() (err error)](#Rows.Close)
  * [func (r Rows) ColumnAutoIncrement(index int) (bool, bool)](#Rows.ColumnAutoIncrement)
  * [func (r Rows) ColumnHidden(index int) (bool, bool)](#Rows.ColumnHidden)
  * [func (r Rows) ColumnKey(index int) (bool, bool)](#Rows.ColumnKey)
  * [func (r Rows) ColumnTypeDatabaseTypeName(index int) string](#Rows.ColumnTypeDatabaseTypeName)
  * [func (r Rows) ColumnTypeLength(index int) (int64, bool)](#Rows.ColumnTypeLength)
  * [func (r Rows) ColumnTypeNullable(index int) (bool, bool)](#Rows.ColumnTypeNullable)
  * [func (r Rows) ColumnTypePrecisionScale(index int) (int64, int64, bool)](#Rows.ColumnTypePrecisionScale)
  * [func (r Rows) ColumnTypeScanType(index int) reflect.Type](#Rows.ColumnTypeScanType)
  * [func (r Rows) Columns() (columns []string)](#Rows.Columns)
  * [func (r Rows) ComputeByList() (list []int, ok bool)](#Rows.ComputeByList)
  * [func (r Rows) ComputedColumnInfo(index int) (operator string, operand int, ok bool)](#Rows.ComputedColumnInfo)
  * [func (r Rows) HasNextResultSet() bool](#Rows.HasNextResultSet)
  * [func (r *Rows) Next(dest []driver.Value) (err error)](#Rows.Next)
  * [func (r *Rows) NextResultSet() error](#Rows.NextResultSet)
  * [func (r Rows) ReturnStatus() (returnStatus int, ok bool)](#Rows.ReturnStatus)
* [type Stmt](#Stmt)
  * [func (st *Stmt) Close() error](#Stmt.Close)
  * [func (st Stmt) ColumnConverter(idx int) driver.ValueConverter](#Stmt.ColumnConverter)
  * [func (st *Stmt) Exec(args []driver.Value) (res driver.Result, err error)](#Stmt.Exec)
  * [func (st *Stmt) ExecContext(ctx context.Context, namedArgs []driver.NamedValue) (res driver.Result, err error)](#Stmt.ExecContext)
  * [func (st Stmt) NumInput() int](#Stmt.NumInput)
  * [func (st *Stmt) Query(args []driver.Value) (driver.Rows, error)](#Stmt.Query)
  * [func (st *Stmt) QueryContext(ctx context.Context, namedArgs []driver.NamedValue) (driver.Rows, error)](#Stmt.QueryContext)
* [type SybError](#SybError)
  * [func (e SybError) Error() string](#SybError.Error)


#### <a name="pkg-files">Package files</a>
[changetype_string.go](/changetype_string.go) [columnflag_string.go](/columnflag_string.go) [datatype_string.go](/datatype_string.go) [doc.go](/doc.go) [driver.go](/driver.go) [messages.go](/messages.go) [num.go](/num.go) [result.go](/result.go) [rows.go](/rows.go) [session.go](/session.go) [stmt.go](/stmt.go) [tabular_messages.go](/tabular_messages.go) [token_string.go](/token_string.go) [types.go](/types.go) 


## <a name="pkg-variables">Variables</a>
``` go
var ErrBadType = errors.New("invalid type given")
```
ErrBadType is raised when trying to convert a value to an incompatible data type.

``` go
var ErrInvalidIsolationLevel = errors.New("tds: invalid or unsupported isolation level")
```
ErrInvalidIsolationLevel is raised when an unsupported isolation level is asked.

``` go
var ErrNoReadOnly = errors.New("tds: readonly is unsupported")
```
ErrNoReadOnly is raise when readonly attribute of driver.TxOptions is set.
Readonly sessions are not supported by sybase.

``` go
var ErrNonNullable = errors.New("trying to insert null values into non null column")
```
ErrNonNullable is raised when trying to insert null values to non-null fields

``` go
var ErrOverFlow = errors.New("overflow when converting to database type")
```
ErrOverFlow is raised when there is an overflow when converting
the parameter to	 a database type.

``` go
var ErrUnsupportedPassWordEncrytion = errors.New("tds: login failed. Unsupported encryption")
```
ErrUnsupportedPassWordEncrytion is caused by an unsupported password encrytion scheme (used by ASE <= 15.0.1)




## <a name="Conn">type</a> [Conn](/driver.go?s=979:1009#L45)
``` go
type Conn struct {
    // contains filtered or unexported fields
}

```
Conn encapsulates a tds session and satisties driver.Connc







### <a name="NewConn">func</a> [NewConn](/driver.go?s=3259:3298#L137)
``` go
func NewConn(dsn string) (*Conn, error)
```
NewConn returns a TDS session





### <a name="Conn.Begin">func</a> (Conn) [Begin](/session.go?s=8409:8453#L314)
``` go
func (s Conn) Begin() (driver.Tx, error)
```



### <a name="Conn.BeginTx">func</a> (Conn) [BeginTx](/session.go?s=9261:9349#L333)
``` go
func (s Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error)
```
BeginTx implements driver.ConnBeginTx interface




### <a name="Conn.Close">func</a> (Conn) [Close](/session.go?s=8083:8114#L300)
``` go
func (s Conn) Close() error
```
Close terminates the session
by sending logout message and closing tcp connection.




### <a name="Conn.Commit">func</a> (Conn) [Commit](/session.go?s=10391:10423#L367)
``` go
func (s Conn) Commit() error
```



### <a name="Conn.Exec">func</a> (Conn) [Exec](/session.go?s=12243:12323#L433)
``` go
func (s Conn) Exec(query string, args []driver.Value) (driver.Result, error)
```
Exec implements the Querier interface.
The aim is to use language queries when no parameters are given




### <a name="Conn.ExecContext">func</a> (Conn) [ExecContext](/session.go?s=12459:12578#L442)
``` go
func (s Conn) ExecContext(ctx context.Context, query string,
    namedArgs []driver.NamedValue) (driver.Result, error)
```
Implement the "ExecerContext" interface




### <a name="Conn.GetEnv">func</a> (Conn) [GetEnv](/driver.go?s=3976:4016#L169)
``` go
func (c Conn) GetEnv() map[string]string
```
GetEnv return a map of environments variables.
The following keys are garanteed to be present:


	- server
	- database
	- charset




### <a name="Conn.Ping">func</a> (Conn) [Ping](/session.go?s=10888:10937#L380)
``` go
func (s Conn) Ping(ctx context.Context) error
```
Ping implements driver.Pinger interface




### <a name="Conn.Prepare">func</a> (Conn) [Prepare](/session.go?s=13047:13107#L467)
``` go
func (s Conn) Prepare(query string) (driver.Stmt, error)
```
Prepare prepares a statement and returns it




### <a name="Conn.PrepareContext">func</a> (Conn) [PrepareContext](/session.go?s=13240:13328#L475)
``` go
func (s Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error)
```
Prepare prepares a statement and returns it




### <a name="Conn.Query">func</a> (Conn) [Query](/session.go?s=11317:11396#L400)
``` go
func (s Conn) Query(query string, args []driver.Value) (driver.Rows, error)
```
Exec implements the Execer interface.
The aim is to use language queries when no parameters are given




### <a name="Conn.QueryContext">func</a> (Conn) [QueryContext](/session.go?s=11533:11651#L408)
``` go
func (s Conn) QueryContext(ctx context.Context, query string,
    namedArgs []driver.NamedValue) (driver.Rows, error)
```
Implement the "QueryerContext" interface




### <a name="Conn.Rollback">func</a> (Conn) [Rollback](/session.go?s=10615:10649#L373)
``` go
func (s Conn) Rollback() error
```



### <a name="Conn.SelectValue">func</a> (Conn) [SelectValue](/session.go?s=13459:13554#L483)
``` go
func (s Conn) SelectValue(ctx context.Context, query string) (value interface{}, err error)
```
Reads exactly one value from an sql query




### <a name="Conn.SetErrorhandler">func</a> (\*Conn) [SetErrorhandler](/driver.go?s=3747:3803#L160)
``` go
func (c *Conn) SetErrorhandler(fn func(s SybError) bool)
```
SetErrorhandler allows setting a custom error handler.
The function shall accept an SQL Message and return a boolean
indicating if this message is indeed a critical error.




## <a name="Errorhandler">type</a> [Errorhandler](/driver.go?s=842:915#L40)
``` go
type Errorhandler interface {
    SetErrorHandler(func(m sqlMessage) bool)
}
```









## <a name="Num">type</a> [Num](/num.go?s=758:844#L32)
``` go
type Num struct {
    // contains filtered or unexported fields
}

```
Num represents a sybase numeric data type










### <a name="Num.Rat">func</a> (Num) [Rat](/num.go?s=3475:3501#L144)
``` go
func (n Num) Rat() big.Rat
```
Rat returns the underlying big.Rat value




### <a name="Num.Scan">func</a> (\*Num) [Scan](/num.go?s=1798:1839#L79)
``` go
func (n *Num) Scan(src interface{}) error
```
Scan implements the Scanner interface.
Allows initiating a tds.Num from a string, or any golang numeric type.
When providing a string, it must be in decimal form,
with an optional sign, ie -50.40
The dot is the separator.

Example:


	num := Num{precision: p, scale: s}
	num.Scan("-10.4")

A loss of precision should alway cause an error (except for bugs, of course).




### <a name="Num.String">func</a> (Num) [String](/num.go?s=2986:3014#L124)
``` go
func (n Num) String() string
```
implement the stringer interface




## <a name="Result">type</a> [Result](/result.go?s=65:521#L9)
``` go
type Result struct {
    // contains filtered or unexported fields
}

```
Result information










### <a name="Result.LastInsertId">func</a> (\*Result) [LastInsertId](/result.go?s=598:644#L30)
``` go
func (r *Result) LastInsertId() (int64, error)
```
LastInsertId returns the id of the last insert.
TODO: handle context




### <a name="Result.RowsAffected">func</a> (Result) [RowsAffected](/result.go?s=1028:1073#L46)
``` go
func (r Result) RowsAffected() (int64, error)
```
RowsAffected returns the number of rows affected by the last statement




## <a name="Rows">type</a> [Rows](/rows.go?s=160:964#L15)
``` go
type Rows struct {
    // contains filtered or unexported fields
}

```
Rows information, columns and data










### <a name="Rows.AffectedRows">func</a> (Rows) [AffectedRows](/rows.go?s=8206:8255#L292)
``` go
func (r Rows) AffectedRows() (count int, ok bool)
```
AffectedRows returns the number of affected rows
Satisfies the driver.Rows interface




### <a name="Rows.Close">func</a> (\*Rows) [Close](/rows.go?s=3265:3299#L115)
``` go
func (r *Rows) Close() (err error)
```
Close skips all remaining rows
NB: only return error on unexpected failure.




### <a name="Rows.ColumnAutoIncrement">func</a> (Rows) [ColumnAutoIncrement](/rows.go?s=9865:9922#L344)
``` go
func (r Rows) ColumnAutoIncrement(index int) (bool, bool)
```
ColumnAutoIncrement returns a boolean indicating if the column is auto-incremented.




### <a name="Rows.ColumnHidden">func</a> (Rows) [ColumnHidden](/rows.go?s=10174:10224#L353)
``` go
func (r Rows) ColumnHidden(index int) (bool, bool)
```
ColumnHidden returns a boolean indicating if the column is hidden.
Sybase returns hidden columns when using "for browse"




### <a name="Rows.ColumnKey">func</a> (Rows) [ColumnKey](/rows.go?s=10426:10473#L361)
``` go
func (r Rows) ColumnKey(index int) (bool, bool)
```
ColumnKey returns a boolean indicating if the column is in the primary key.




### <a name="Rows.ColumnTypeDatabaseTypeName">func</a> (Rows) [ColumnTypeDatabaseTypeName](/rows.go?s=8686:8744#L307)
``` go
func (r Rows) ColumnTypeDatabaseTypeName(index int) string
```
ColumnTypeDatabaseTypeName returns the sybase type name as a string.
Satisfies the driver.Rows interface




### <a name="Rows.ColumnTypeLength">func</a> (Rows) [ColumnTypeLength](/rows.go?s=8969:9024#L316)
``` go
func (r Rows) ColumnTypeLength(index int) (int64, bool)
```
ColumnTypeLength returns the length of a column given by its index.
Satisfies the driver.Rows interface




### <a name="Rows.ColumnTypeNullable">func</a> (Rows) [ColumnTypeNullable](/rows.go?s=9245:9301#L325)
``` go
func (r Rows) ColumnTypeNullable(index int) (bool, bool)
```
ColumnTypeNullable returns the nullability of a column given by its index.
Satisfies the driver.Rows interface




### <a name="Rows.ColumnTypePrecisionScale">func</a> (Rows) [ColumnTypePrecisionScale](/rows.go?s=9565:9635#L334)
``` go
func (r Rows) ColumnTypePrecisionScale(index int) (int64, int64, bool)
```
ColumnTypePrecisionScale returns the precision and scale of a numeric column given by its index.
Satisfies the driver.Rows interface




### <a name="Rows.ColumnTypeScanType">func</a> (Rows) [ColumnTypeScanType](/rows.go?s=8418:8474#L298)
``` go
func (r Rows) ColumnTypeScanType(index int) reflect.Type
```
ColumnTypeScanType returns the value type to scan into.
Satisfies the driver.Rows interface




### <a name="Rows.Columns">func</a> (Rows) [Columns](/rows.go?s=2886:2928#L99)
``` go
func (r Rows) Columns() (columns []string)
```
Columns returns the resultset's columns
Satisfies the driver.Rows interface




### <a name="Rows.ComputeByList">func</a> (Rows) [ComputeByList](/rows.go?s=7760:7811#L277)
``` go
func (r Rows) ComputeByList() (list []int, ok bool)
```
ComputeByList the list of columns in the "by" clause of a compute

the result is an array containing the indices.
This result is valid only after the computed row was returned.
See ComputedColumnInfo() for the reason




### <a name="Rows.ComputedColumnInfo">func</a> (Rows) [ComputedColumnInfo](/rows.go?s=7268:7351#L264)
``` go
func (r Rows) ComputedColumnInfo(index int) (operator string, operand int, ok bool)
```
ComputedColumnInfo returns the operator and the operand
for a computed column, given its index

This result is valid only after the computed row was returned.
Indeed, a statement can contain several compute clause.
Sybase sends compute inforamtion tokens, along with an ID to match the row
and the relevant columns' information.
Here we only handle the last computed result received from the wire,
as those are overriden in the row handling routine.




### <a name="Rows.HasNextResultSet">func</a> (Rows) [HasNextResultSet](/rows.go?s=3788:3825#L141)
``` go
func (r Rows) HasNextResultSet() bool
```
HasNextResultSet indicates of there is a second result set pending




### <a name="Rows.Next">func</a> (\*Rows) [Next](/rows.go?s=4313:4365#L160)
``` go
func (r *Rows) Next(dest []driver.Value) (err error)
```
Next implements the driver.Result Next method to fetch the next row

It will return io.EOF at the end of the result set
If another resultset is found, sets the hasNextResultSet property to true.




### <a name="Rows.NextResultSet">func</a> (\*Rows) [NextResultSet](/rows.go?s=3959:3995#L147)
``` go
func (r *Rows) NextResultSet() error
```
NextResultSet resets the hasNextResultSet
to trigger the processing at the next call to Next()




### <a name="Rows.ReturnStatus">func</a> (Rows) [ReturnStatus](/rows.go?s=7999:8055#L286)
``` go
func (r Rows) ReturnStatus() (returnStatus int, ok bool)
```
ReturnStatus returns the last return status for the current resultset.
Satisfies the driver.Rows interface




## <a name="Stmt">type</a> [Stmt](/stmt.go?s=241:507#L16)
``` go
type Stmt struct {
    ID int64
    // contains filtered or unexported fields
}

```
Stmt is a prepared statement implementing the driver.Stmt interface










### <a name="Stmt.Close">func</a> (\*Stmt) [Close](/stmt.go?s=5646:5675#L207)
``` go
func (st *Stmt) Close() error
```
Close drops the prepared statement from the database




### <a name="Stmt.ColumnConverter">func</a> (Stmt) [ColumnConverter](/stmt.go?s=6237:6298#L226)
``` go
func (st Stmt) ColumnConverter(idx int) driver.ValueConverter
```
ColumnConverter returns converters which check min, max, nullability,
precision, scale and then convert to a valid sql.Driver value.




### <a name="Stmt.Exec">func</a> (\*Stmt) [Exec](/stmt.go?s=3291:3363#L131)
``` go
func (st *Stmt) Exec(args []driver.Value) (res driver.Result, err error)
```
Exec executes a prepared statement.
Implements the database/sql/Stmt interface




### <a name="Stmt.ExecContext">func</a> (\*Stmt) [ExecContext](/stmt.go?s=3854:3964#L150)
``` go
func (st *Stmt) ExecContext(ctx context.Context, namedArgs []driver.NamedValue) (res driver.Result, err error)
```
ExecContext executes a prepared statement, along with a context.
Implements the database/sql/Stmt interface




### <a name="Stmt.NumInput">func</a> (Stmt) [NumInput](/stmt.go?s=5527:5556#L202)
``` go
func (st Stmt) NumInput() int
```
NumInput returns the number of expected parameters




### <a name="Stmt.Query">func</a> (\*Stmt) [Query](/stmt.go?s=4589:4652#L175)
``` go
func (st *Stmt) Query(args []driver.Value) (driver.Rows, error)
```
Query executes a prepared statement and returns rows.




### <a name="Stmt.QueryContext">func</a> (\*Stmt) [QueryContext](/stmt.go?s=4995:5096#L186)
``` go
func (st *Stmt) QueryContext(ctx context.Context, namedArgs []driver.NamedValue) (driver.Rows, error)
```
QueryContext executes a prepared statement and returns rows




## <a name="SybError">type</a> [SybError](/messages.go?s=19514:19782#L903)
``` go
type SybError struct {
    MsgNumber  int32
    State      int8
    Severity   int8
    SQLState   string // 1 byte size
    HasEed     uint8
    TranState  uint16
    Message    string // 2 bytes size
    Server     string // 1 byte size
    Procedure  string // 1 byte size
    LineNumber int16
}

```
SybError is the struct containing sybase error information










### <a name="SybError.Error">func</a> (SybError) [Error](/messages.go?s=19817:19849#L917)
``` go
func (e SybError) Error() string
```
implement the error interface





