package tds

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"os"
	"testing"
)

// implementation checks
var _ driver.Conn = (*session)(nil)
var _ driver.Execer = (*session)(nil)
var _ driver.ExecerContext = (*session)(nil)
var _ driver.Queryer = (*session)(nil)
var _ driver.QueryerContext = (*session)(nil)
var _ driver.Pinger = (*session)(nil)
var _ driver.ConnPrepareContext = (*session)(nil)
var _ driver.ConnBeginTx = (*session)(nil)

var _ driver.StmtExecContext = (*Stmt)(nil)
var _ driver.StmtQueryContext = (*Stmt)(nil)

var _ driver.RowsColumnTypeLength = (*Rows)(nil)
var _ driver.RowsColumnTypeDatabaseTypeName = (*Rows)(nil)
var _ driver.RowsColumnTypeNullable = (*Rows)(nil)
var _ driver.RowsColumnTypePrecisionScale = (*Rows)(nil)
var _ driver.RowsColumnTypeScanType = (*Rows)(nil)
var _ driver.RowsNextResultSet = (*Rows)(nil)

var (
	testDatabase   = "master"
	testHostname   string
	testUserName   string
	testChained    = false
	testPacketSize = 512
	testCharset    = "utf8"
	testPassword   string
)

func buildurl() string {
	// build the url
	v := url.Values{}
	database := os.Getenv("TDS_DATABASE")

	if database == "" {
		database = "master"
	}

	if testChained {
		v.Set("mode", "chained")
	}

	if testPacketSize != 0 {
		v.Set("packetSize", fmt.Sprintf("%d", testPacketSize))
	}

	v.Set("encryptPassword", "yes")

	v.Set("hostname", testHostname)
	if testCharset != "" {
		v.Set("charset", testCharset)
	}

	return "tds://" + url.QueryEscape(os.Getenv("TDS_USERNAME")) + ":" + url.QueryEscape(os.Getenv("TDS_PASSWORD")) + "@" +
		os.Getenv("TDS_SERVER") + "/" + database + "?" + v.Encode()
}

// connect to the database and return it
func connect(t *testing.T) *sql.DB {
	db, err := sql.Open("syb", buildurl())
	db.SetMaxOpenConns(15)
	db.SetMaxIdleConns(1)
	if err != nil {
		t.Fatal("sql.Open failed:", err.Error())
		return nil
	}
	if err = db.Ping(); err != nil {
		t.Error("sql.Ping failed:", err.Error())
		return nil
	}
	return db
}

// connect to the database and return it
func connectb(b *testing.B) *sql.DB {
	db, err := sql.Open("syb", buildurl())
	if err != nil {
		b.Error("sql.Open failed:", err.Error())
		return nil
	}
	if err = db.Ping(); err != nil {
		b.Error("sql.Ping failed:", err.Error())
		return nil
	}
	return db
}

// get the underlying driver
func getConn(t *testing.T) *Conn {
	db := connect(t)
	if db == nil {
		return nil
	}
	drv, _ := db.Driver().Open(buildurl())
	return drv.(*Conn)
}

// test connection
func TestConnect(t *testing.T) {
	db, err := sql.Open("syb", buildurl())
	if err != nil {
		t.Error("sql.Open failed:", err.Error())
		return
	}
	if err = db.Ping(); err != nil {
		t.Error("sql.Ping failed:", err.Error())
		return
	}
	defer db.Close()
}

// check that the database parameter was given
func TestInitDb(t *testing.T) {
	drv := getConn(t)
	if drv == nil {
		return
	}
	if os.Getenv("TDS_DATABASE") != "" && drv.session.database != os.Getenv("TDS_DATABASE") {
		t.Errorf("should be in database %s, in %s instead", os.Getenv("TDS_DATABASE"), drv.session.database)
	}
}

// scramble the vars, connect should fail
func TestBadConnect(t *testing.T) {
	// reset to the correct value afterwards
	old := os.Getenv("TDS_PASSWORD")
	defer os.Setenv("TDS_PASSWORD", old)

	os.Setenv("TDS_PASSWORD", "TDS_PASSWORD")
	db, err := sql.Open("syb", buildurl())
	if err != nil {
		t.Error("sql.Open failed:", err.Error())
		return
	}
	defer db.Close()
	err = db.Ping()
	if err == nil || err.(SybError).MsgNumber != 4002 {
		t.Error("ping should fail with a sybase login error")
	}
}
