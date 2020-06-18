package tds

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
)

func TestSimpleExec(t *testing.T) {
	db := getConn(t)
	if db == nil {
		return
	}
	defer db.Close()
	res, err := db.simpleExec(context.Background(), "set textsize 5000")
	if err != nil {
		t.Error("Conn.simpleExec failed:", err.Error())
		return
	}
	if cnt, _ := res.RowsAffected(); cnt != 0 {
		t.Errorf("Conn.simpleExec: Result.RowsAffected: exepected 0 rows affected, got %d", cnt)
		return
	}
}

func TestSingleValue(t *testing.T) {
	db := getConn(t)
	if db == nil {
		return
	}
	defer db.Close()
	res, err := db.SelectValue(context.Background(), "select 1")
	if err != nil {
		t.Error("Conn.selectValue failed:", err.Error())
		return
	}
	intVal, ok := res.(int64)
	if !ok {
		t.Errorf("Conn.selectValue, expected int, got %s", reflect.TypeOf(res))
		return
	}
	if intVal != 1 {
		t.Errorf("Conn.selectValue, expected 1, got %d", intVal)
		return
	}
}

func TestTransactions(t *testing.T) {
	db := getConn(t)
	if db == nil {
		return
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Error("Conn.Begin failed:", err.Error())
		return
	}
	res, err := db.SelectValue(context.Background(), "select @@trancount")
	if err != nil {
		t.Error("Conn.Begin: Conn.selectValue failed:", err.Error())
		return
	}
	intVal, ok := res.(int64)
	if !ok {
		t.Errorf("Conn.Begin: Conn.selectValue, expected int, got %s", reflect.TypeOf(res))
		return
	}
	if intVal != 1 {
		t.Errorf("Conn.Begin: Conn.selectValue, expected 1, got %d", intVal)
		return
	}
	tx.Rollback()
	res, err = db.SelectValue(context.Background(), "select @@trancount")
	if err != nil {
		t.Error("Conn.Rollback: Conn.selectValue failed:", err.Error())
		return
	}
	intVal, ok = res.(int64)
	if !ok {
		t.Errorf("Conn.Rollback: Conn.selectValue, expected int, got %s", reflect.TypeOf(res))
		return
	}
	if intVal != 0 {
		t.Errorf("Conn.Rollback: Conn.selectValue, expected 0, got %d", intVal)
		return
	}
	tx, err = db.Begin()
	if err != nil {
		t.Error("Conn.Begin failed:", err.Error())
		return
	}
	res, err = db.SelectValue(context.Background(), "select @@trancount -- begin")
	if err != nil {
		t.Error("Conn.Begin: Conn.selectValue failed:", err.Error())
		return
	}
	intVal, ok = res.(int64)
	if !ok {
		t.Errorf("Conn.Begin: Conn.selectValue, expected int, got %s", reflect.TypeOf(res))
		return
	}
	if intVal != 1 {
		t.Errorf("Conn.Begin: Conn.selectValue, expected 1, got %d", intVal)
		return
	}
	tx.Rollback()
	res, err = db.SelectValue(context.Background(), "select @@trancount -- rollback")
	if err != nil {
		t.Error("Conn.Rollback: Conn.selectValue failed:", err.Error())
		return
	}
	intVal, ok = res.(int64)
	if !ok {
		t.Errorf("Conn.Rollback: Conn.selectValue, expected int, got %s", reflect.TypeOf(res))
		return
	}
	if intVal != 0 {
		t.Errorf("Conn.Rollback: Conn.selectValue, expected 0, got %d", intVal)
		return
	}
}

type isolationTest struct {
	input  *sql.TxOptions
	err    error
	output int
}

var isolationTests = []isolationTest{
	{input: &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelDefault}, err: ErrNoReadOnly},
	{input: &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelReadUncommitted}, err: nil, output: 0},
	{input: &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelReadCommitted}, err: nil, output: 1},
	{input: &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelRepeatableRead}, err: nil, output: 2},
	{input: &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelSerializable}, err: nil, output: 3},
	{input: &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelSnapshot}, err: ErrInvalidIsolationLevel},
	{input: &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelLinearizable}, err: ErrInvalidIsolationLevel},
}

// test all the transaction levels
func TestIsolationLevels(t *testing.T) {
	db := connect(t)
	if db == nil {
		return
	}
	defer db.Close()

	for _, value := range isolationTests {
		tx, err := db.BeginTx(context.Background(), value.input)
		if err != value.err {
			t.Fatal(err)
		}

		if value.err != nil {
			continue
		}

		var isolation int
		//err = tx.QueryRow("select @@isolation").Scan(&isolation)
		err = tx.QueryRow("select @@isolation").Scan(&isolation)
		if err != nil {
			t.Fatal(err)
		}

		if value.output != isolation {
			t.Errorf("wrong isolation level: %d != %d", isolation, value.output)
		}

		var trancount int
		//err = tx.QueryRow("select @@isolation").Scan(&isolation)
		err = tx.QueryRow("select @@trancount").Scan(&trancount)
		if err != nil {
			t.Fatal(err)
		}

		if trancount != 1 {
			t.Error("Expected @@trancount set to 1")
		}

		tx.Rollback()
	}

}

func TestBeginTranError(t *testing.T) {
	conn := getConn(t)
	if conn == nil {
		t.Fatal("connect failed")
	}

	defer conn.Close()
	// close actual connection to make begin transaction to fail during sending of a packet
	conn.c.Close()

	ctx := context.Background()
	opts := driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelSerializable)}
	_, err := conn.BeginTx(ctx, opts)
	if err == nil || conn.valid {
		t.Errorf("begin should fail as a bad connection, err=%v", err)
	}
}

func TestCommitTranError(t *testing.T) {
	conn := getConn(t)
	if conn == nil {
		t.Fatal("connect failed")
	}

	defer conn.Close()
	// close actual connection to make begin transaction to fail during sending of a packet
	conn.c.Close()

	err := conn.Commit()
	if err == nil || conn.valid {
		t.Errorf("begin should fail as a bad connection, err=%v", err)
	}
}

func TestRollbackTranError(t *testing.T) {
	conn := getConn(t)
	if conn == nil {
		t.Fatal("connect failed")
	}

	defer conn.Close()
	// close actual connection to make begin transaction to fail during sending of a packet
	conn.c.Close()

	err := conn.Rollback()
	if err == nil || conn.valid {
		t.Errorf("begin should fail as a bad connection, err=%v", err)
	}
}

func TestBeginTxtReadOnlyNotSupported(t *testing.T) {
	conn := connect(t)
	if conn == nil {
		t.Fatal("connect failed")
	}
	defer conn.Close()
	opts := &sql.TxOptions{ReadOnly: true}
	_, err := conn.BeginTx(context.Background(), opts)
	if err == nil {
		t.Error("BeginTx expected to fail for read only transaction because sybase doesn't support it, but it succeeded")
	}
}

func TestConn_BeginTx(t *testing.T) {
	conn := connect(t)
	defer conn.Close()
	conn.Exec("drop table test")

	_, err := conn.Exec("create table test (f int)")
	if err != nil {
		t.Fatal("create table failed with error", err)
	}

	defer conn.Exec("drop table test")

	tx1, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal("BeginTx failed with error", err)
	}
	tx2, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal("BeginTx failed with error", err)
	}
	_, err = tx1.Exec("insert into test (f) values (1)")
	if err != nil {
		t.Fatal("insert failed with error", err)
	}
	tx1.Rollback()

	_, err = tx2.Exec("insert into test (f) values (2)")
	if err != nil {
		t.Fatal("insert failed with error", err)
	}
	tx2.Commit()

	rows, err := conn.Query("select f from test")
	if err != nil {
		t.Fatal("select failed with error", err)
	}
	values := []int64{}
	for rows.Next() {
		var val int64
		err = rows.Scan(&val)
		if err != nil {
			t.Fatal("scan failed with error", err)
		}
		values = append(values, val)
	}
	if !reflect.DeepEqual(values, []int64{2}) {
		t.Errorf("Values is expected to be [1] but it is %v", values)
	}
}

func TestPinger(t *testing.T) {
	conn := connect(t)
	defer conn.Close()
	err := conn.Ping()
	if err != nil {
		t.Errorf("Failed to hit database")
	}
}
