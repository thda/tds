/*
Package tds is a pure Go Sybase ASE/IQ/RS driver for the database/sql package.

Status

This is a beta release. This driver has yet to be battle tested on
production workload. Version 1.0 will be released
when this driver will be production ready

Requirements

 - Sybase ASE 12.5 or higher
 - go 1.8 or higher.


Installation

Package installation is done via go-get:

	$ go get -u github.com/thda/tds

Usage

It implements most of the database/sql functionalities.
To connect to a sybase instance, import the package and
use the regular database/sql APIs:

	import (
		"database/sql"

		_ "github.com/thda/tds"
	)

	func main() {
		cnxStr := "tds://my_user:my_password@dbhost.com:5000/pubs?charset=utf8"
		db, err := sql.Open("tds", cnxStr)
		if err != nil {
			log.Fatal(err)
		}

		id := 2
		rows, err := db.Query("select * from authors where id = ?", id)
		…
	}

Connection String

The connection string is pretty standard and uses the URL format:

	tds://username:password@host:port/database?parameter=value&parameter2=value2

Connection parameters

The most common ones are:

 - username - the database server login. Mandatory.
 - password - The login's password. Mandatory.
 - host - The host to connect to. Mandatory.
 - port - The port to bind to. Mandatory.
 - database - The database to use. You will connect to the login's
   default database if not specified.
 - charset - The client's character set. Default to utf8.
   Please refer to the character sets section.
 - readTimeout - read timeout in seconds.
 - writeTimeout - write timeout in seconds.
 - textSize - max size of textsize fields in bytes.
 It is suggested to raise it to avoid truncation.

Less frequently used ones:

 - ssl - Whether or not to use SSL. The default is not to use ssl.
   Set to "on" if the server is setup to use ssl.
 - encryptPassword - Can be "yes" to require password encryption,
   "no" to disable it, and "try" to try encrytping password an falling back
   to plain text password. Password encryption works on Sybase ASE 15.5
   or higher and uses RSA.
 - packetSize - Network packet size. Must be less than or equal the server's
   max network packet size. The default is the server's default network
   packet size.
 - applicationName - the name of your application.
   It is a best practice to set it.

Query parameters

Most of the database/sql APIs are implemented, with a major one missing:
named parameters. Please use the question mark '?' as a placeholder
for parameters :

		res, err = tx.Exec("insert into author (id, name) values (?, ?)", 2, "Paul")

Supported data types

Almost all of the sybase ASE datatypes are supported,
with the exception of lob locators.
The type mapping between the server and the go data types is as follows:

 - varchar/text/char/unichar/univarchar/xml => string
 - int/smalling/bigint => int64.
   Unsigned bigints with a value > Math.MaxInt64 will be returned as uint64
 - date/datetime/bigdate/bigdatetime => time.Time
 - image/binary/varbinary/image => []byte
 - real/float => float64
 - decimal/numeric/money/smallmoney => tds.Num.
   Please see the  "precise numerical types" section.

Precise numerical types

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

Character set encoding

This driver assumes by default that the client uses utf8 strings and will
ask the server to convert back and forth to/from this charset.

If utf8 charset conversion is not supported on the server, and if the
charset conversion is not supported by golang.org/x/text/encoding,
client-side character set conversion will not be possible.

You will have to handle it yourself and use a charset supported by the server.

Custom error handling

One can set a custom error callback to process server errors before
the regular error processing routing.
This allows handling showplan messages and print statement.

The following demonstrates how to handle showplan and print messages:

	conn := sql.Open("tds", url)

	// print showplan messages and all
	conn.Driver().(tds.ErrorHandler).SetErrorhandler(func(m tds.SybError) bool {
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

Limitations

As of now the driver does not support bulk insert and named parameters.
Password encryption only works for Sybase ASE > 15.5.

Testing

You can use stmt_test.go and session_test.go for sample usage, as follows:

	export TDS_USERNAME=test_user
	export TDS_PASSWORD=test_password
	export TDS_SERVER=localhost:5000
	go test

License

This driver is release under the go license

Credits

 - the freetds and jtds protocol documentation.
 - Microsoft for releasing the full tds specification.
   There are differences, however a lot of it is relevant.
 - github.com/denisenkom/go-mssqldb for most of the tests.
 - The Sybase::TdsServer perl module for capabilities handling.

*/
package tds
