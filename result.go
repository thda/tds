package tds

import (
	"fmt"
	"strconv"
)

// Result information
type Result struct {
	s *session
	// number of rows affected by the last command
	affectedRows int64
	// return status
	returnStatus int
	// an error was reported by the done token
	hasError bool
	// the affected rows were returned
	hasAffectedRows bool
	// a return status was received
	hasReturnStatus bool
	// it's the last message of the response. Found in done tokens
	final bool
	// server messages and errors
	messages  []SybError
	lastError error
}

// LastInsertId returns the id of the last insert.
// TODO: handle context
func (r *Result) LastInsertId() (int64, error) {
	val, err := r.s.SelectValue(nil, "select @@identity")
	if err != nil {
		return 0, fmt.Errorf("tds: identity fetch failed: %s", err)
	}
	if i, ok := val.(int64); ok {
		return i, nil
	}
	if n, ok := val.(Num); ok {
		j, err := strconv.Atoi(n.String())
		return int64(j), err
	}
	return val.(int64), nil
}

// RowsAffected returns the number of rows affected by the last statement
func (r Result) RowsAffected() (int64, error) {
	if r.hasAffectedRows {
		return r.affectedRows, nil
	}
	return 0, nil
}
