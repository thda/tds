package tds

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	"errors"
)

func TestExec(t *testing.T) {
	db := connect(t)
	if db == nil {
		t.Fatal("connect failed")
	}
	defer db.Close()
	if _, err := db.Exec("print 'test'"); err != nil {
		t.Fatal(err)
	}
}

func TestQueryRow(t *testing.T) {
	db := connect(t)
	if db == nil {
		t.Fatal("connect failed")
	}
	defer db.Close()

	// check value
	var i int
	if err := db.QueryRow("select 1").Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expected %d, got %d", 1, i)
	}

	// an insert should return ErrNoRows
	if err := db.QueryRow("print 'test'").Scan(); err != sql.ErrNoRows {
		t.Fatalf("QueryRow, expected sql.ErrNoRows, got %s", err)
	}

	// check value
	var i2 uint64
	if err := db.QueryRow("select convert(unsigned bigint,18446744073709551615)").Scan(&i2); err != nil {
		t.Fatal(err)
	}
	if i2 != 18446744073709551615 {
		t.Fatalf("expected %d, got %d", 1, i)
	}
}

func TestMultipleInserts(t *testing.T) {
	db := connect(t)
	if db == nil {
		t.Fatal("connect failed")
	}
	defer db.Close()
	// drop if necessary
	db.Exec("drop table tempdb..testTds")

	// test simple exec
	if _, err := db.Exec("create table tempdb..testTds (testCol int null)"); err != nil {
		t.Fatal(err)
	}

	// check with prepare and multiple execs
	stmt, err := db.Prepare("insert into tempdb..testTds (testCol) values (?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < 5; i++ {
		if _, err := stmt.Exec(i); err != nil {
			t.Fatal(err)
		}
	}
}

type testValue struct {
	value      interface{} // input value
	columnDesc string
	err        error       // expected error
	output     interface{} // expected output value
}

var ErrorNotNil = fmt.Errorf("not nil error")

func TestConverters(t *testing.T) {
	db := connect(t)
	if db == nil {
		t.Fatal("connect failed")
	}
	defer db.Close()

	// numerics for tests
	var num1 = Num{precision: 12, scale: 4}
	num1.Scan("144.3")

	var num2 = Num{precision: 12, scale: 4}
	num2.Scan("144")

	testValues := []testValue{
		// int
		testValue{value: int64(3230), err: nil, columnDesc: "int not null", output: int64(3230)},
		testValue{value: math.MaxInt32 + 1, err: ErrOverFlow, columnDesc: "int not null", output: nil},
		testValue{value: math.MinInt32 - 1, err: ErrOverFlow, columnDesc: "int not null", output: nil},
		testValue{value: int64(math.MaxInt32), err: nil, columnDesc: "int not null", output: int64(math.MaxInt32)},
		testValue{value: int64(math.MinInt32), err: nil, columnDesc: "int not null", output: int64(math.MinInt32)},
		testValue{value: nil, err: nil, columnDesc: "int null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "int not null", output: nil},

		// smallint
		testValue{value: int64(3230), err: nil, columnDesc: "smallint not null", output: int64(3230)},
		testValue{value: math.MaxInt16 + 1, err: ErrOverFlow, columnDesc: "smallint null", output: nil},
		testValue{value: math.MinInt16 - 1, err: ErrOverFlow, columnDesc: "smallint null", output: nil},
		testValue{value: int64(math.MaxInt16), err: nil, columnDesc: "smallint not null", output: int64(math.MaxInt16)},
		testValue{value: int64(math.MinInt16), err: nil, columnDesc: "smallint not null", output: int64(math.MinInt16)},
		testValue{value: nil, err: nil, columnDesc: "smallint null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "smallint not null", output: nil},

		// date
		testValue{value: time.Date(1900, time.Month(1), 1, 0, 0, 0, 0, time.Local), err: nil, columnDesc: "date not null", output: time.Date(1900, time.Month(1), 1, 0, 0, 0, 0, time.Local)},
		testValue{value: time.Date(0000, time.Month(12), 31, 0, 0, 0, 0, time.Local), err: ErrOverFlow, columnDesc: "date not null", output: nil},
		testValue{value: time.Date(10000, time.Month(1), 1, 0, 0, 0, 0, time.Local), err: ErrOverFlow, columnDesc: "date not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "date null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "date not null", output: nil},

		// bit
		testValue{value: false, err: nil, columnDesc: "bit", output: false},
		testValue{value: true, err: nil, columnDesc: "bit", output: true},
		testValue{value: nil, err: ErrNonNullable, columnDesc: "bit", output: false},

		// binary
		testValue{value: nil, err: nil, columnDesc: "binary(1) null", output: nil},
		testValue{value: []byte{00, 01}, err: nil, columnDesc: "binary(1) null", output: []byte{00}},
		testValue{value: "nil", err: ErrBadType, columnDesc: "binary(1) null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "binary(1) not null", output: nil},

		// varbinary
		testValue{value: []byte{00, 01}, err: nil, columnDesc: "varbinary(2) null", output: []byte{00, 01}},
		testValue{value: nil, err: nil, columnDesc: "varbinary(2) null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "varbinary(2) not null", output: nil},

		// datetime
		testValue{value: time.Date(1900, time.Month(1), 1, 0, 0, 0, 0, time.Local), err: nil, columnDesc: "datetime not null", output: time.Date(1900, time.Month(1), 1, 0, 0, 0, 0, time.Local)},
		testValue{value: time.Date(1752, time.Month(12), 31, 0, 0, 0, 0, time.Local), err: ErrOverFlow, columnDesc: "datetime not null", output: nil},
		testValue{value: time.Date(10000, time.Month(1), 1, 0, 0, 0, 0, time.Local), err: ErrOverFlow, columnDesc: "datetime not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "datetime null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "datetime not null", output: nil},

		// smalldatetime
		testValue{value: time.Date(1900, time.Month(1), 1, 0, 0, 0, 0, time.Local), err: nil, columnDesc: "smalldatetime not null", output: time.Date(1900, time.Month(1), 1, 0, 0, 0, 0, time.Local)},
		testValue{value: time.Date(1899, time.Month(12), 31, 0, 0, 0, 0, time.Local), err: ErrOverFlow, columnDesc: "smalldatetime not null", output: nil},
		testValue{value: time.Date(2080, time.Month(1), 1, 0, 0, 0, 0, time.Local), err: ErrOverFlow, columnDesc: "smalldatetime not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "smalldatetime null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "smalldatetime not null", output: nil},

		// bigdatetime
		testValue{value: time.Date(1900, time.Month(1), 1, 0, 0, 0, 999999000, time.Local), err: nil, columnDesc: "bigdatetime not null", output: time.Date(1900, time.Month(1), 1, 0, 0, 0, 999999000, time.Local)},
		testValue{value: time.Date(0000, time.Month(12), 31, 0, 0, 0, 0, time.Local), err: ErrOverFlow, columnDesc: "bigdatetime not null", output: nil},
		testValue{value: time.Date(10000, time.Month(1), 1, 0, 0, 0, 0, time.Local), err: ErrOverFlow, columnDesc: "bigdatetime not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "bigdatetime null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "bigdatetime not null", output: nil},

		// bigtime
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "bigtime not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "bigtime null", output: nil},
		testValue{value: time.Date(1990, time.Month(10), 12, 22, 55, 01, 999999000, time.Local), err: nil, columnDesc: "bigtime not null", output: time.Date(1900, time.Month(1), 1, 22, 55, 01, 999999000, time.Local)},

		// time
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "time not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "time null", output: nil},
		testValue{value: time.Date(1990, time.Month(10), 12, 22, 55, 01, 0, time.Local), err: nil, columnDesc: "time not null", output: time.Date(1900, time.Month(1), 1, 22, 55, 01, 0, time.Local)},

		// numeric
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "numeric(12,4) not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "numeric(12,4) null", output: nil},
		testValue{value: num1, err: nil, columnDesc: "numeric(12,4) not null", output: num1},
		testValue{value: num1, err: ErrOverFlow, columnDesc: "numeric(10,4) not null", output: nil},
		testValue{value: int64(144), err: nil, columnDesc: "numeric(12,4) not null", output: num2},

		// real
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "real not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "real null", output: nil},
		testValue{value: float64(math.MaxFloat64), err: ErrOverFlow, columnDesc: "real not null", output: nil},
		testValue{value: float32(1442.212), err: nil, columnDesc: "real not null", output: float32(1442.212)},
		testValue{value: float32(214748.35), err: nil, columnDesc: "real not null", output: float32(214748.35)},

		// float
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "float not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "float null", output: nil},
		testValue{value: float64(1442.212), err: nil, columnDesc: "float not null", output: float64(1442.212)},

		// bigint
		testValue{value: int64(3230), err: nil, columnDesc: "bigint not null", output: int64(3230)},
		testValue{value: uint64(math.MaxInt64 + 1), err: ErrOverFlow, columnDesc: "bigint not null", output: nil},
		testValue{value: int64(math.MaxInt64), err: nil, columnDesc: "bigint not null", output: int64(math.MaxInt64)},
		testValue{value: int64(math.MinInt64), err: nil, columnDesc: "bigint not null", output: int64(math.MinInt64)},
		testValue{value: nil, err: nil, columnDesc: "bigint null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "bigint not null", output: nil},

		// tinyint
		testValue{value: int64(200), err: nil, columnDesc: "tinyint not null", output: int64(200)},
		testValue{value: int64(math.MaxUint8 + 1), err: ErrOverFlow, columnDesc: "tinyint not null", output: nil},
		testValue{value: int64(math.MaxUint8), err: nil, columnDesc: "tinyint not null", output: int64(math.MaxUint8)},
		testValue{value: int64(-1), err: ErrOverFlow, columnDesc: "tinyint not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "tinyint null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "tinyint", output: nil},

		// unsigned int
		testValue{value: int64(3230), err: nil, columnDesc: "unsigned int not null", output: int64(3230)},
		testValue{value: math.MaxUint32 + 1, err: ErrOverFlow, columnDesc: "unsigned int not null", output: nil},
		testValue{value: -1, err: ErrOverFlow, columnDesc: "unsigned int not null", output: nil},
		testValue{value: int64(math.MaxUint32), err: nil, columnDesc: "unsigned int not null", output: int64(math.MaxUint32)},
		testValue{value: nil, err: nil, columnDesc: "unsigned int null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "unsigned int not null", output: nil},

		// unsigned smallint
		testValue{value: int64(3230), err: nil, columnDesc: "unsigned smallint not null", output: int64(3230)},
		testValue{value: math.MaxUint16 + 1, err: ErrOverFlow, columnDesc: "unsigned smallint null", output: nil},
		testValue{value: -1, err: ErrOverFlow, columnDesc: "unsigned smallint null", output: nil},
		testValue{value: int64(math.MaxUint16), err: nil, columnDesc: "unsigned smallint not null", output: int64(math.MaxUint16)},
		testValue{value: nil, err: nil, columnDesc: "unsigned smallint null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "unsigned smallint not null", output: nil},

		// unsigned bigint
		testValue{value: uint64(3230), err: nil, columnDesc: "unsigned bigint not null", output: uint64(3230)},
		testValue{value: -1, err: ErrOverFlow, columnDesc: "unsigned bigint not null", output: nil},
		testValue{value: uint64(math.MaxUint64), err: nil, columnDesc: "unsigned bigint not null", output: uint64(math.MaxUint64)},
		testValue{value: nil, err: nil, columnDesc: "unsigned bigint null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "unsigned bigint not null", output: nil},

		// tinyint
		testValue{value: int64(200), err: nil, columnDesc: "unsigned tinyint not null", output: int64(200)},
		testValue{value: int64(math.MaxUint8 + 1), err: ErrOverFlow, columnDesc: "unsigned tinyint not null", output: nil},
		testValue{value: int64(math.MaxUint8), err: nil, columnDesc: "unsigned tinyint not null", output: int64(math.MaxUint8)},
		testValue{value: int64(-1), err: ErrOverFlow, columnDesc: "unsigned tinyint not null", output: nil},
		testValue{value: nil, err: nil, columnDesc: "unsigned tinyint null", output: nil},
		testValue{value: nil, err: errors.New("Msg: 515"), columnDesc: "unsigned tinyint not null", output: nil},

		// money/smallmoney
		testValue{value: getNum("214748.35", 10, 4), err: nil, columnDesc: "smallmoney", output: getNum("214748.35", 10, 4)},
		testValue{value: getNum("922337203685477.56", 19, 4), err: nil, columnDesc: "money", output: getNum("922337203685477.56", 19, 4)},
		testValue{value: getNum("922337203685477.5807", 19, 4), err: nil, columnDesc: "money null", output: getNum("922337203685477.5807", 19, 4)},
		testValue{value: getNum("922337203685477.56", 19, 4), err: nil, columnDesc: "money", output: getNum("922337203685477.56", 19, 4)},
		testValue{value: getNum("-922337203685477.56", 19, 4), err: nil, columnDesc: "money", output: getNum("-922337203685477.56", 19, 4)},
		testValue{value: getNum("214748.35", 10, 4), err: nil, columnDesc: "smallmoney", output: getNum("214748.35", 10, 4)},
		testValue{value: getNum("-214748.35", 10, 4), err: nil, columnDesc: "smallmoney", output: getNum("-214748.35", 10, 4)},
	}

	var output interface{}
	for _, value := range testValues {
		// drop if necessary
		db.Exec("drop table #testTds")

		// test simple exec
		if _, err := db.Exec("create table #testTds (testCol " + value.columnDesc + ")"); err != nil {
			t.Fatal(err)
		}

		stmt, err := db.Prepare(`insert into #testTds values(?)
							select * from #testTds `)

		if err != nil {
			t.Fatalf("Error while preparing statement: %s", err)
		}
		rows, err := stmt.Query(value.value)

		// compare query error with
		if err != nil {
			if value.err == nil {
				t.Fatalf("unexpected error %s", err)
			} else if !strings.Contains(err.Error(), value.err.Error()) {
				t.Fatalf("expected error %s, got %s", value.err, err)
			}
			stmt.Close()
			continue
		}

		// scan
		ok := rows.Next()

		if !ok {
			if value.err == nil {
				t.Fatalf("unexpected error while scanning row: %s", rows.Err())
			}
			if !strings.Contains(rows.Err().Error(), value.err.Error()) {
				t.Fatalf("expected error %s, got %s", value.err, err)
			}
			stmt.Close()
			continue
		}

		err = rows.Scan(&output)
		rows.Close()

		if err != nil {
			t.Fatalf("Error while scanning value: %s", err)
		}

		// compare values with stringer when possible, sometime slight differences cause DeepEqual to fail
		// mainly the tds.Num.r rational taken from a pool
		if str, ok := value.output.(fmt.Stringer); ok {
			if str2, ok := output.(fmt.Stringer); ok {
				if str.String() != str2.String() {
					t.Fatalf("expected stringer value %s, got %s", value.output, output)
				}
			} else {
				t.Fatalf("expected stringer %#v, got non stringer %#v", value.output, output)
			}
		} else {
			// compare the values
			if !reflect.DeepEqual(value.output, output) {
				t.Fatalf("expected value %#v, got %#v", spew.Sdump(value.output), spew.Sdump(output))
			}
		}
		stmt.Close()
	}
}

func BenchmarkMultipleInserts(b *testing.B) {
	db := connectb(b)
	if db == nil {
		b.Fatal("connect failed")
	}
	defer db.Close()

	// drop if necessary
	db.Exec("drop table #testTds")

	// test simple exec
	if _, err := db.Exec("create table #testTds (testCol int null, testCol2 int null, testCol3 varchar(10), testCol4 int)"); err != nil {
		b.Fatal(err)
	}

	// check with prepare and multiple execs
	stmt, err := db.Prepare("insert into #testTds (testCol, testCol2, testCol3, testCol4) values (?,?,?,?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := int64(0); i < int64(b.N); i++ {
		if _, err := stmt.Exec(i, i, "test"+strconv.Itoa(int(i)), i); err != nil {
			b.Fatal(err)
		}
	}
}

func TestQueryCancel(t *testing.T) {
	db := connect(t)
	if db == nil {
		t.Fatal("connect failed")
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	_, err := db.ExecContext(ctx, "waitfor delay '00:00:03'")
	if err != context.Canceled {
		t.Errorf("ExecContext expected to fail with Cancelled but it returned %v", err)
	}

	// connection should be usable after Timeout
	row := db.QueryRow("select 1")
	var val int64
	err = row.Scan(&val)

	if err != nil {
		t.Fatal("Scan failed with", err)
	}
}

func getNum(str string, p int8, s int8) Num {
	num := Num{precision: p, scale: s}
	num.Scan(str)
	return num
}

func TestSelect(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}
	defer conn.Close()

	t.Run("scan into interface{}", func(t *testing.T) {
		type testStruct struct {
			sql string
			val interface{}
		}

		longstr := strings.Repeat("x", 10000)

		values := []testStruct{
			{"1", int64(1)},
			{"-1", int64(-1)},
			{"cast(1 as int)", int64(1)},
			{"cast(-1 as int)", int64(-1)},
			{"cast(1 as tinyint)", int64(1)},
			{"cast(255 as tinyint)", int64(255)},
			{"cast(1 as smallint)", int64(1)},
			{"cast(-1 as smallint)", int64(-1)},
			{"cast(1 as bigint)", int64(1)},
			{"cast(-1 as bigint)", int64(-1)},
			{"cast(1 as bit)", true},
			{"cast(0 as bit)", false},
			{"'abc'", string("abc")},
			{"cast(0.5 as float)", float64(0.5)},
			{"cast(0.5 as real)", float32(0.5)},
			{"cast(1.2345 as money)", getNum("1.2345", 20, 4)},
			{"cast(-1.2345 as money)", getNum("-1.2345", 20, 4)},
			{"cast(1.2345 as smallmoney)", getNum("1.2345", 10, 4)},
			{"cast(-1.2345 as smallmoney)", getNum("-1.2345", 10, 4)},
			{"cast(0.5 as decimal(18,1))", getNum("0.5", 18, 1)},
			{"cast(-0.5 as decimal(18,1))", getNum("-0.5", 18, 1)},
			{"cast(-0.5 as numeric(18,1))", getNum("-0.5", 18, 1)},
			{"cast(4294967296 as numeric(20,0))", getNum("4294967296", 20, 0)},
			{"cast(-0.5 as numeric(18,2))", getNum("-0.5", 18, 2)},
			{"'abc'", string("abc")},
			{"cast(null as varchar(3))", nil},
			{"NULL", nil},
			{"cast('1753-01-01' as datetime)", time.Date(1753, 1, 1, 0, 0, 0, 0, time.Local)},
			{"cast('2000-01-01' as datetime)", time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local)},
			{"cast('2000-01-01T12:13:14.12' as datetime)",
				time.Date(2000, 1, 1, 12, 13, 14, 120000000, time.Local)},
			{"cast('2014-06-26 11:08:09.673' as datetime)", time.Date(2014, 06, 26, 11, 8, 9, 673000000, time.Local)},
			{"cast('9999-12-31T23:59:59.996' as datetime)", time.Date(9999, 12, 31, 23, 59, 59, 996000000, time.Local)},
			{"cast(NULL as datetime)", nil},
			{"cast('1900-01-01T00:00:00' as smalldatetime)",
				time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local)},
			{"cast('2000-01-01T12:13:00' as smalldatetime)",
				time.Date(2000, 1, 1, 12, 13, 0, 0, time.Local)},
			{"cast('2079-06-06T23:59:00' as smalldatetime)",
				time.Date(2079, 6, 6, 23, 59, 0, 0, time.Local)},
			{"cast(NULL as smalldatetime)", nil},
			{"cast(0x1234 as varbinary(2))", []byte{0x12, 0x34}},
			{"cast(null as unitext)", nil},
			{"cast('abc' as text)", "abc"},
			{"cast(null as text)", nil},
			{"cast('abc' as unitext)", "abc"},
			{"cast(0x1234 as image)", []byte{0x12, 0x34}},
			{"cast('abc' as char(3))", "abc"},
			{"cast('abc' as varchar(3))", "abc"},
			{"cast('проверка' as univarchar)", "проверка"},
			{"cast('Δοκιμή' as univarchar)", "Δοκιμή"},
			{"cast('สวัสดี' as univarchar)", "สวัสดี"},
			{"cast('你好' as univarchar)", "你好"},
			{"cast('こんにちは' as univarchar)", "こんにちは"},
			{"cast('안녕하세요.' as univarchar)", "안녕하세요."},
			{"cast('你好' as univarchar)", "你好"},
			{"cast('cześć' as univarchar)", "cześć"},
			{"cast('Алло' as univarchar)", "Алло"},
			{"cast('Bonjour' as univarchar)", "Bonjour"},
			{"cast('Γεια σας' as univarchar)", "Γεια σας"},
			{"cast('Merhaba' as univarchar)", "Merhaba"},
			{"cast('שלום' as univarchar)", "שלום"},
			{"cast('مرحبا' as univarchar)", "مرحبا"},
			{"cast('Sveiki' as univarchar)", "Sveiki"},
			{"cast('chào' as univarchar)", "chào"},
			{fmt.Sprintf("cast('%s' as varchar(10000))", longstr), longstr},
		}

		for _, test := range values {
			t.Run(test.sql, func(t *testing.T) {
				stmt, err := conn.Prepare("select " + test.sql)
				if err != nil {
					t.Error("Prepare failed:", test.sql, err.Error())
					return
				}
				defer stmt.Close()

				row := stmt.QueryRow()
				var retval interface{}
				err = row.Scan(&retval)
				if err != nil {
					t.Error("Scan failed:", test.sql, err.Error())
					return
				}
				var same bool
				switch decodedval := retval.(type) {
				case []byte:
					switch decodedvaltest := test.val.(type) {
					case []byte:
						same = bytes.Equal(decodedval, decodedvaltest)
					default:
						same = false
					}
				default:
					// compare values with stringer when possible, sometime slight differences cause DeepEqual to fail
					// mainly the tds.Num.r rational taken from a pool
					if str, ok := retval.(fmt.Stringer); ok {
						if str2, ok := test.val.(fmt.Stringer); ok {
							same = str.String() == str2.String()
						} else {
							t.Fatalf("expected stringer %#v, got non stringer %#v", retval, test.val)
						}
					} else {
						same = reflect.DeepEqual(retval, test.val)
					}
				}
				if !same {
					t.Errorf("Values don't match '%s' '%s' for test: %s", retval, test.val, test.sql)
					return
				}
			})
		}
	})
	t.Run("scan into *int64", func(t *testing.T) {
		t.Run("from integer", func(t *testing.T) {
			row := conn.QueryRow("select 11")
			var retval *int64
			err := row.Scan(&retval)
			if err != nil {
				t.Error("Scan failed", err.Error())
				return
			}
			if *retval != 11 {
				t.Errorf("Expected 11, got %v", retval)
			}
		})
		t.Run("from null", func(t *testing.T) {
			row := conn.QueryRow("select null")
			var retval *int64
			err := row.Scan(&retval)
			if err != nil {
				t.Error("Scan failed", err.Error())
				return
			}
			if retval != nil {
				t.Errorf("Expected nil, got %v", retval)
			}
		})
	})
}

func TestNull(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}
	defer conn.Close()

	t.Run("scan into interface{}", func(t *testing.T) {
		types := []string{
			"tinyint",
			"smallint",
			"int",
			"bigint",
			"real",
			"float",
			"smallmoney",
			"money",
			"decimal",
			//"varbinary(15)",
			//"binary(15)",
			"varchar(15)",
			"char(15)",
			"smalldatetime",
			"date",
			"time",
			"datetime",
		}
		for _, typ := range types {
			t.Run(typ, func(t *testing.T) {
				row := conn.QueryRow("create table #test ( x "+typ+" null)\n"+
					"insert into #test values (?)\n"+
					"select * from #test", nil)
				var retval interface{}
				err := row.Scan(&retval)
				if err != nil {
					t.Error("Scan failed for type "+typ, err.Error())
					return
				}
				conn.Exec("drop table #test", nil)
				if retval != nil {
					t.Error("Value should be nil, but it is ", retval)
					return
				}
			})
		}
	})

	t.Run("scan into NullInt64", func(t *testing.T) {
		types := []string{
			"tinyint",
			"smallint",
			"int",
			"bigint",
			"real",
			"float",
			"smallmoney",
			"money",
			"decimal",
			//"varbinary(15)",
			//"binary(15)",
			"varchar(15)",
			"char(15)",
			"smalldatetime",
			"date",
			"time",
			"datetime",
		}
		for _, typ := range types {
			row := conn.QueryRow("create table #test ( x "+typ+" null)\n"+
				"insert into #test values (?)\n"+
				"select * from #test", nil)
			var retval sql.NullInt64
			err := row.Scan(&retval)
			if err != nil {
				t.Error("Scan failed for type "+typ, err.Error())
				return
			}
			if retval.Valid {
				t.Error("Value should be nil, but it is ", retval)
				return
			}
		}
	})

	t.Run("scan into *int", func(t *testing.T) {
		types := []string{
			"tinyint",
			"smallint",
			"int",
			"bigint",
			"real",
			"float",
			"smallmoney",
			"money",
			"decimal",
			//"varbinary(15)",
			//"binary(15)",
			"nvarchar(15)",
			"nchar(15)",
			"varchar(15)",
			"char(15)",
			"smalldatetime",
			"date",
			"time",
			"datetime",
		}
		for _, typ := range types {
			row := conn.QueryRow("create table #test ( x "+typ+" null)\n"+
				"insert into #test values (?)\n"+
				"select * from #test", nil)
			var retval *int
			err := row.Scan(&retval)
			if err != nil {
				t.Error("Scan failed for type "+typ, err.Error())
				return
			}
			if retval != nil {
				t.Error("Value should be nil, but it is ", retval)
				return
			}
		}
	})
}

func TestError(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}

	_, err := conn.Query("exec bad")
	if err == nil {
		t.Fatal("Query should fail")
	}

	if sqlerr, ok := err.(SybError); !ok {
		t.Fatalf("Should be sql error, actually %T, %v", err, err)
	} else {
		if sqlerr.MsgNumber != 2812 { // Could not find stored procedure 'bad'
			t.Fatalf("Should be specific error code 2812, actually %d %s", sqlerr.MsgNumber, sqlerr)
		}
	}
}

func TestQueryNoRows(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}

	var rows *sql.Rows
	var err error
	if rows, err = conn.Query("create table #abc (fld int)"); err != nil {
		t.Fatal("Query failed", err)
	}
	if rows.Next() {
		t.Fatal("Query shoulnd't return any rows")
	}
}

func TestQueryManyNullsRow(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}

	var row *sql.Row
	var err error
	if row = conn.QueryRow("select null, null, null, null, null, null, null, null"); err != nil {
		t.Fatal("Query failed", err)
	}
	var v [8]sql.NullInt64
	if err = row.Scan(&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7]); err != nil {
		t.Fatal("Scan failed", err)
	}
}

func TestAffectedRows(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}

	res, err := conn.Exec("create table #foo (bar int)")
	if err != nil {
		t.Fatal("create table failed")
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal("Begin tran failed", err)
	}
	defer tx.Rollback()

	res, err = tx.Exec("select * from #foo")
	if err != nil {
		t.Fatal("select table failed", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatal("rows affected failed")
	}
	if n != 0 {
		t.Error("Expected 0 rows affected, got ", n)
	}

	res, err = tx.Exec("insert into #foo (bar) values (1)")
	if err != nil {
		t.Fatal("insert failed", err)
	}
	n, err = res.RowsAffected()
	if err != nil {
		t.Fatal("rows affected failed")
	}
	if n != 1 {
		t.Error("Expected 1 row affected, got ", n)
	}
	res, err = tx.Exec("insert into #foo (bar) values (?)", 2)
	if err != nil {
		t.Fatal("insert failed", err)
	}
	n, err = res.RowsAffected()
	if err != nil {
		t.Fatal("rows affected failed", err)
	}
	if n != 1 {
		t.Error("Expected 1 row affected, got ", n)
	}
}

func TestIdentity(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}

	res, err := conn.Exec("create table #foo (bar int identity, baz int)")
	if err != nil {
		t.Fatal("create table failed")
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal("Begin tran failed", err)
	}
	defer tx.Rollback()

	res, err = tx.Exec("insert into #foo (baz) values (1)")
	if err != nil {
		t.Fatal("insert failed", err)
	}
	n, err := res.LastInsertId()
	if err != nil {
		t.Fatal("last insert id failed")
	}
	if n != 1 {
		t.Error("Expected 1 for identity, got ", n)
	}

	res, err = tx.Exec("insert into #foo (baz) values (20)")
	if err != nil {
		t.Fatal("insert failed")
	}
	n, err = res.LastInsertId()
	if err != nil {
		t.Fatal("last insert id failed")
	}
	if n != 2 {
		t.Error("Expected 2 for identity, got ", n)
	}
}

func queryParamRoundTrip(db *sql.DB, param interface{}, dest interface{}) {
	err := db.QueryRow(`
	delete #foo
	insert into #foo values (?)
	select * from #foo
	`, param).Scan(dest)
	if err != nil {
		log.Panicf("select / scan failed: %v", err.Error())
	}
}

func TestDateTimeParam(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}
	type testStruct struct {
		t time.Time
	}

	_, err := conn.Exec("create table #foo (bar bigdatetime)")
	if err != nil {
		t.Fatal("create table failed")
	}

	mindate := time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)
	maxdate := time.Date(9999, 12, 31, 23, 59, 59, 999999900, time.Local)
	values := []testStruct{
		{time.Date(1969, time.July, 20, 20, 18, 0, 0, time.Local)}, // First man on the Moon
		{time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)},            // UNIX date
		{time.Date(4, 6, 3, 12, 13, 14, 150000000, time.Local)},    // some random date
		{mindate}, // minimal value
	}
	for _, test := range values {
		t.Run(fmt.Sprintf("Test for %v", test.t), func(t *testing.T) {
			var t2 time.Time
			queryParamRoundTrip(conn, test.t, &t2)
			expected := test.t
			// clip value
			if test.t.Before(mindate) {
				expected = mindate
			}
			if test.t.After(maxdate) {
				expected = maxdate
			}

			if expected.Sub(t2) != 0 {
				t.Errorf("expected: '%s', got: '%s' delta: %d", expected, t2, expected.Sub(t2))
			}
		})
	}

}

func TestIgnoreEmptyResults(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}
	rows, err := conn.Query(`set nocount on
	select 2`)
	if err != nil {
		t.Fatal("Query failed", err.Error())
	}
	if !rows.Next() {
		t.Fatal("Query didn't return row")
	}
	var fld1 int32
	err = rows.Scan(&fld1)
	if err != nil {
		t.Fatal("Scan failed", err)
	}
	if fld1 != 2 {
		t.Fatal("Returned value doesn't match")
	}
}

func TestConnectionClosing(t *testing.T) {
	pool := connect(t)
	if pool == nil {
		t.Fatal("connect failed")
	}
	defer pool.Close()
	for i := 1; i <= 100; i++ {
		if pool.Stats().OpenConnections > 1 {
			t.Errorf("Open connections is expected to stay <= 1, but it is %d", pool.Stats().OpenConnections)
			return
		}

		stmt, err := pool.Query("select 1")
		if err != nil {
			t.Fatalf("Query failed with unexpected error %s", err)
		}
		for stmt.Next() {
			var val interface{}
			err := stmt.Scan(&val)
			if err != nil {
				t.Fatalf("Query failed with unexpected error %s", err)
			}
		}
	}
}

func TestSendQueryErrors(t *testing.T) {
	conn := getConn(t)
	if conn == nil {
		t.Fatal("connect failed")
	}

	defer conn.Close()
	stmt, err := conn.PrepareContext(context.Background(), "select 1")
	if err != nil {
		t.FailNow()
	}

	// should fail because parameter is invalid
	_, err = stmt.Query([]driver.Value{conn})
	if err == nil {
		t.Fail()
	}

	// close actual connection to make commit transaction to fail during sending of a packet
	conn.c.Close()

	// should fail because connection is closed
	_, err = stmt.Query([]driver.Value{})
	if err == nil || conn.isBad == false {
		t.Fail()
	}

	// should fail because connection is closed
	_, err = stmt.Query([]driver.Value{int64(1)})
	if err == nil || conn.isBad == false {
		t.Fail()
	}
}

func TestSendExecErrors(t *testing.T) {
	conn := getConn(t)
	if conn == nil {
		t.Fatal("connect failed")
	}

	defer conn.Close()
	stmt, err := conn.PrepareContext(context.Background(), "select 1")
	if err != nil {
		t.Fatal("prepare failed")
	}

	// should fail because parameter is invalid
	_, err = stmt.Exec([]driver.Value{conn})
	if err == nil {
		t.Fatal("exec should fail", err)
	}

	// close actual connection to make commit transaction to fail during sending of a packet
	conn.c.Close()

	// should fail because connection is closed
	_, err = stmt.Exec([]driver.Value{})
	if err == nil || conn.isBad == false {
		t.Fatal("exec should fail", err)
	}

	// should fail because connection is closed
	_, err = stmt.Exec([]driver.Value{int64(1)})
	if err == nil || conn.isBad == false {
		t.Fail()
	}
}

func TestNextResultSet(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}
	rows, err := conn.QueryContext(context.Background(), `select 1
	select 2`)
	if err != nil {
		t.Fatal("Query failed", err.Error())
	}

	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Query didn't return row")
	}
	var fld1, fld2 int32
	err = rows.Scan(&fld1)
	if err != nil {
		t.Fatal("Scan failed", err)
	}
	if fld1 != 1 {
		t.Fatal("Returned value doesn't match")
	}
	if rows.Next() {
		t.Fatal("Query returned unexpected second row.")
	}
	// calling next again should still return false
	if rows.Next() {
		t.Fatal("Query returned unexpected second row.")
	}
	if !rows.NextResultSet() {
		t.Fatal("NextResultSet should return true but returned false")
	}
	if !rows.Next() {
		t.Fatal("Query didn't return row")
	}
	err = rows.Scan(&fld2)
	if err != nil {
		t.Fatal("Scan failed", err)
	}
	if fld2 != 2 {
		t.Fatal("Returned value doesn't match")
	}
	if rows.NextResultSet() {
		t.Fatal("NextResultSet should return false but returned true")
	}
}

func TestColumnTypeIntrospection(t *testing.T) {
	type tst struct {
		expr         string
		typeName     string
		reflType     reflect.Type
		hasSize      bool
		size         int64
		hasPrecScale bool
		precision    int64
		scale        int64
	}
	tests := []tst{
		{"cast(1 as bit)", "bit", reflect.TypeOf(true), false, 0, false, 0, 0},
		{"cast(1 as tinyint)", "tinyint", reflect.TypeOf(int64(0)), false, 0, false, 0, 0},
		{"cast(1 as smallint)", "smallint", reflect.TypeOf(int64(0)), false, 0, false, 0, 0},
		{"1", "int", reflect.TypeOf(int64(0)), false, 0, false, 0, 0},
		{"cast(1 as bigint)", "bigint", reflect.TypeOf(int64(0)), false, 0, false, 0, 0},
		{"cast(1 as real)", "real", reflect.TypeOf(0.0), false, 0, false, 0, 0},
		{"cast(1 as float)", "float", reflect.TypeOf(0.0), false, 0, false, 0, 0},
		{"cast('abc' as varbinary(3))", "varbinary", reflect.TypeOf([]byte{}), true, 3, false, 0, 0},
		{"cast('abc' as varbinary(200))", "varbinary", reflect.TypeOf([]byte{}), true, 200, false, 0, 0},
		{"cast(1 as datetime)", "datetime", reflect.TypeOf(time.Time{}), false, 0, false, 0, 0},
		{"cast(1 as smalldatetime)", "smalldatetime", reflect.TypeOf(time.Time{}), false, 0, false, 0, 0},
		{"cast(getdate() as date)", "date", reflect.TypeOf(time.Time{}), false, 0, false, 0, 0},
		{"cast(getdate() as time)", "time", reflect.TypeOf(time.Time{}), false, 0, false, 0, 0},
		{"cast('abc' as varchar(200))", "varchar", reflect.TypeOf(""), true, 200, false, 0, 0},
		{"'abc'", "varchar", reflect.TypeOf(""), true, 3, false, 0, 0},
		{"cast(1 as decimal)", "decimal", reflect.TypeOf(Num{}), false, 0, true, 18, 0},
		{"cast(1 as decimal(5, 2))", "decimal", reflect.TypeOf(Num{}), false, 0, true, 5, 2},
		{"cast(1 as numeric(10, 4))", "numeric", reflect.TypeOf(Num{}), false, 0, true, 10, 4},
		{"cast(1 as money)", "money", reflect.TypeOf(Num{}), false, 0, false, 0, 0},
		{"cast(1 as smallmoney)", "smallmoney", reflect.TypeOf(Num{}), false, 0, false, 0, 0},
		{"cast('abc' as text)", "text", reflect.TypeOf(""), true, 65536, false, 0, 0},
		{"cast('abc' as unitext)", "unitext", reflect.TypeOf(""), true, 32768, false, 0, 0},
		{"cast('abc' as image)", "image", reflect.TypeOf([]byte{}), true, 2147483647, false, 0, 0},
		{"cast('abc' as char(3))", "char", reflect.TypeOf(""), true, 3, false, 0, 0},
	}

	conn := connect(t)
	defer conn.Close()
	for _, tt := range tests {
		rows, err := conn.Query("select " + tt.expr)
		if err != nil {
			t.Fatalf("Query failed with unexpected error %s", err)
		}
		ct, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("Query failed with unexpected error %s", err)
		}
		if ct[0].DatabaseTypeName() != tt.typeName {
			t.Errorf("%s: expected type %s but returned %s", tt.expr, tt.typeName, ct[0].DatabaseTypeName())
		}
		size, ok := ct[0].Length()
		if ok != tt.hasSize {
			t.Errorf("Expected has size %v but returned %v for %s", tt.hasSize, ok, tt.expr)
		} else {
			if ok && size != tt.size {
				t.Errorf("Expected size %d but returned %d for %s", tt.size, size, tt.expr)
			}
		}

		prec, scale, ok := ct[0].DecimalSize()
		if ok != tt.hasPrecScale {
			t.Errorf("Expected has prec/scale %v but returned %v for %s", tt.hasPrecScale, ok, tt.expr)
		} else {
			if ok && prec != tt.precision {
				t.Errorf("Expected precision %d but returned %d for %s", tt.precision, prec, tt.expr)
			}
			if ok && scale != tt.scale {
				t.Errorf("Expected scale %d but returned %d for %s", tt.scale, scale, tt.expr)
			}
		}

		if ct[0].ScanType() != tt.reflType {
			t.Errorf("Expected ScanType %v but got %v for %s", tt.reflType, ct[0].ScanType(), tt.expr)
		}
	}
}

func TestColumnIntrospection(t *testing.T) {
	type tst struct {
		expr         string
		fieldName    string
		typeName     string
		nullable     bool
		hasSize      bool
		size         int64
		hasPrecScale bool
		precision    int64
		scale        int64
	}
	tests := []tst{
		{"f1 int null", "f1", "int", true, false, 0, false, 0, 0},
		{"f2 varchar(15) not null", "f2", "varchar", false, true, 15, false, 0, 0},
		{"f3 decimal(5, 2) null", "f3", "decimal", true, false, 0, true, 5, 2},
	}
	conn := connect(t)
	defer conn.Close()

	// making table variable with specified fields and making a select from it
	exprs := make([]string, len(tests))
	for i, test := range tests {
		exprs[i] = test.expr
	}
	exprJoined := strings.Join(exprs, ",")
	rows, err := conn.Query(fmt.Sprintf("create table #foo (%s)\nselect * from #foo", exprJoined))
	if err != nil {
		t.Fatalf("Query failed with unexpected error %s", err)
	}

	ct, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("ColumnTypes failed with unexpected error %s", err)
	}
	for i, test := range tests {
		if ct[i].Name() != test.fieldName {
			t.Errorf("Field expected have name %s but it has name %s", test.fieldName, ct[i].Name())
		}

		if ct[i].DatabaseTypeName() != test.typeName {
			t.Errorf("Invalid type name returned %s expected %s", ct[i].DatabaseTypeName(), test.typeName)
		}

		nullable, ok := ct[i].Nullable()
		if ok {
			if nullable != test.nullable {
				t.Errorf("Invalid nullable value returned %v", nullable)
			}
		} else {
			t.Error("Nullable was expected to support Nullable but it didn't")
		}

		size, ok := ct[i].Length()
		if ok != test.hasSize {
			t.Errorf("Expected has size %v but returned %v for %s", test.hasSize, ok, test.expr)
		} else {
			if ok && size != test.size {
				t.Errorf("Expected size %d but returned %d for %s", test.size, size, test.expr)
			}
		}

		prec, scale, ok := ct[i].DecimalSize()
		if ok != test.hasPrecScale {
			t.Errorf("Expected has prec/scale %v but returned %v for %s", test.hasPrecScale, ok, test.expr)
		} else {
			if ok && prec != test.precision {
				t.Errorf("Expected precision %d but returned %d for %s", test.precision, prec, test.expr)
			}
			if ok && scale != test.scale {
				t.Errorf("Expected scale %d but returned %d for %s", test.scale, scale, test.expr)
			}
		}
	}
}

func TestContext(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	opts := &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	}
	ctx := context.Background()
	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		t.Errorf("BeginTx failed with unexpected error %s", err)
		return
	}

	row := tx.QueryRowContext(ctx, "select 1")
	var val int64
	if err = row.Scan(&val); err != nil {
		t.Errorf("QueryRowContext failed with unexpected error %s", err)
	}
	if val != 1 {
		t.Error("Incorrect value returned from query")
	}

	_, err = tx.ExecContext(ctx, "select 1")
	if err != nil {
		t.Errorf("ExecContext failed with unexpected error %s", err)
		return
	}

	_, err = tx.PrepareContext(ctx, "select 1")
	if err != nil {
		t.Errorf("PrepareContext failed with unexpected error %s", err)
		return
	}
}
