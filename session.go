package tds

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/thda/tds/netlib"

	"errors"
)

var validHost = regexp.MustCompile("([[:alpha:]]|[_.-])*:[0-9]+$")

// ErrUnsupportedPassWordEncrytion is caused by an unsupported password encrytion scheme (used by ASE <= 15.0.1)
var ErrUnsupportedPassWordEncrytion = errors.New("tds: login failed. Unsupported encryption")

// non configurable logout Timeout
var logoutTimeout = 5

var isError = func(s SybError) bool {
	return s.Severity > 10
}

// tds session.
//
// this struct actually implements the protocol,
// sends queries, processes answers
// It also embeds the response/query structs
type session struct {
	valid       bool
	charConvert bool
	res         *Result // response info

	// tds buffer to split into TDS PDUs, send and read packets
	b            *netlib.Buffer
	c            io.ReadWriteCloser // net connection
	capabilities capabilities       // tds capabilities

	// parameters
	packetSize   int
	readTimeout  int
	writeTimeout int
	loginTimeout int

	// tds env
	database   string
	charset    string
	language   string
	server     string
	serverType string

	// tokens for reuse
	envChange    envChange
	done         done
	returnStatus returnStatus
	sqlMessage   sqlMessage

	// previous token
	prev token

	messageMap map[byte]netlib.Messager

	// error handling routine
	IsError func(SybError) bool
}

// instantiate a login sctruct
func newLogin(prm connParams) *login {
	// default values
	l := &login{library: defaultLibrary, protocolVersion: defaultProtocolVersion,
		libraryVersion: defaultLibraryVersion, charset: prm.charset,
		clientHost: prm.clientHost, user: prm.user,
		encrypted: loginSecEncrypt1 | loginSecEncrypt2 | loginSecNonce,
		app:       prm.app, packetSize: prm.packetSize, pid: prm.pid}
	if prm.encryptPassword == "no" {
		l.encrypted = 0
		l.password, l.password2 = prm.password, prm.password
	}
	return l
}

// dial the connection, init the TDS buffer, attempt login
func newSession(prm connParams) (s *session, err error) {
	s = &session{envChange: envChange{msg: newMsg(EnvChange)},
		done:         done{msg: newMsg(Done)},
		sqlMessage:   sqlMessage{msg: newMsg(SQLMessage)},
		returnStatus: returnStatus{msg: newMsg(ReturnStatus)},
		IsError:      isError, packetSize: prm.packetSize,
		readTimeout: prm.readTimeout, writeTimeout: prm.writeTimeout,
		loginTimeout: prm.loginTimeout, res: &Result{lastError: nil}}

	// init resultset, buffer, parameters, message cache...
	s.res.s = s
	s.server = prm.host
	s.messageMap = map[byte]netlib.Messager{byte(EnvChange): &s.envChange,
		byte(DoneProc): &s.done, byte(DoneInProc): &s.done,
		byte(Done): &s.done, byte(ReturnStatus): &s.returnStatus,
		byte(SQLMessage): &s.sqlMessage}

	// connect
	if s.c, err = dial(prm); err != nil {
		return s, err
	}

	// init netlib buffer
	s.b = netlib.NewBuffer(s.packetSize, s.c)
	s.b.ReadTimeout, s.b.WriteTimeout = s.readTimeout, s.writeTimeout
	s.b.DefaultMessageMap = s.messageMap

	// now log in
	if err = s.login(prm); err != nil {
		// retry without password encryption
		if err == ErrUnsupportedPassWordEncrytion && prm.encryptPassword == "try" {
			s.c.Close()
			prm.encryptPassword = "no"
			return newSession(prm)
		}
		return s, err
	}

	return s, nil
}

// dial connects to the target host and returns a writer.
func dial(prm connParams) (io.ReadWriteCloser, error) {
	if prm.ssl == "on" {
		return tls.DialWithDialer(&net.Dialer{Timeout: time.Duration(prm.loginTimeout) * time.Second},
			"tcp", prm.host, &tls.Config{InsecureSkipVerify: true})
	}

	return net.DialTimeout("tcp", prm.host,
		time.Duration(prm.loginTimeout)*time.Second)
}

// login sends the login packets. Login and capabilities required.
// If asked, it will also handle password encryption.
func (s *session) login(prm connParams) (err error) {
	login := newLogin(prm)
	login.msg = msg{flags: fixedSize}
	s.capabilities = *newCapabilities()
	s.capabilities.msg = newMsg(Capabilities)
	login.setCapabilities(s.capabilities)

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(s.loginTimeout)*time.Second)
	defer cancel()

	// send the login
	if err = s.b.Send(ctx, netlib.Login, login, &login.capabilities); err != nil {
		return fmt.Errorf("tds: login send failed: %s", err)
	}
	s.clearResult()

	loginAck := &loginAck{msg: newMsg(LoginAck)}
	pf := &columns{msg: newMsg(ParamFmt)}
	p := &row{msg: newMsg(Param)}

	// only retry once
	try := 0
	var t byte
	// get login ack/auth challenge message
loginResponse:
	for {
		t, err = s.processResponse(ctx,
			map[byte]netlib.Messager{byte(LoginAck): loginAck,
				byte(Capabilities): &s.capabilities,
				byte(ParamFmt):     pf,
				byte(Param):        p}, true)

		switch token(t) {
		case Done:
			break loginResponse
		case ParamFmt:
			// bind the param descriptor and the param
			p.columns = pf.fmts
		}

		if err != nil {
			break
		}
	}

	if err != nil && err != io.EOF {
		return err
	}

	// RSA encryption supported, extract the public key
	// only 1 try
	if len(p.data) > 0 && try == 0 {
		// check if server supports RSA encryption
		if rsa, ok := p.data[0].(int64); ok {
			if rsa != 1 {
				return ErrUnsupportedPassWordEncrytion
			}
		} else {
			return ErrUnsupportedPassWordEncrytion
		}

		// get rsa public key, and encrypt
		try = 1
		block, _ := pem.Decode(p.data[1].([]byte))
		if block == nil {
			return ErrUnsupportedPassWordEncrytion
		}

		var pk rsa.PublicKey
		_, err := asn1.Unmarshal(block.Bytes, &pk)
		if err != nil {
			return ErrUnsupportedPassWordEncrytion
		}

		// nonce introduces randomness to avoid replay attacks
		nonce := p.data[2].([]byte)
		message := append(nonce, []byte(prm.password)...)
		ciphertext, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, &pk, message, []byte{})
		if err != nil {
			return fmt.Errorf("tds: login failed. Cannot encrypt password: %s", err)
		}

		// send the encrypted password

		// unknown sybMsg for now, help welcome
		msg1 := &sybMsg{msg: newMsg(Msg), field1: 0x01, field2: 0x0001F}
		msg2 := &sybMsg{msg: newMsg(Msg), field1: 0x01, field2: 0x0020}

		cols1 := &columns{msg: newMsg(ParamFmt), fmts: []colFmt{
			colFmt{colType: getType(LongBinary, 2147483647)},
		}}
		row1 := &row{msg: newMsg(Param), data: []driver.Value{ciphertext},
			columns: cols1.fmts[:]}

		cols2 := &columns{msg: newMsg(ParamFmt), fmts: []colFmt{
			colFmt{colType: getType(Varchar, 255)},
			colFmt{colType: getType(LongBinary, 2147483647)},
		}}
		row2 := &row{msg: newMsg(Param), data: []driver.Value{"", ciphertext},
			columns: cols2.fmts[:]}

		if err = s.b.Send(ctx, netlib.Normal, msg1, cols1, row1, msg2, cols2, row2); err != nil {
			return fmt.Errorf("tds: login send failed: %s", err)
		}

		// re-read, we should get login ack now
		goto loginResponse
	}

	if loginAck.ack != 5 {
		return errors.New("tds: login failed. Please check username/password")
	}
	// we are logged in
	s.valid = true

	// keep the server name provided in the loginAck
	s.serverType = loginAck.server

	// use the proper database
	if prm.database != "" {
		if _, err = s.simpleExec(ctx, "use "+prm.database); err != nil {
			return fmt.Errorf("tds: use database failed: %s", err)
		}
	}

	return err
}

// checkErr check if the given error is fatal.
// If the error is not a sybase error message,
// but another unknown error, mark the connection as bad.
// If the root cause is EOF or a context cancelled,
// simply rethrow it so that driver can catch them.
func (s *session) checkErr(err error, msg string, ignoreEOF bool) error {
	if !s.valid {
		return driver.ErrBadConn
	}
	// fastpath for io.EOF
	switch err {
	case nil:
		return nil
	case io.EOF:
		if ignoreEOF {
			return nil
		}
		return io.EOF
	case context.Canceled, context.DeadlineExceeded:
		return err
	}

	// if the error is not a standard sybase message,
	// the connection is invalid
	if _, ok := err.(SybError); !ok {
		s.valid = false
	}
	return fmt.Errorf("%s: %s", msg, err)
}

// Close terminates the session
// by sending logout message and closing tcp connection.
func (s *session) Close() error {
	defer s.c.Close()

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(logoutTimeout)*time.Second)
	defer cancel()

	if err := s.b.Send(ctx, netlib.Normal,
		logout{msg: newMsg(Logout)}); err != nil {
		return fmt.Errorf("tds: close failed: %s", err)
	}
	return nil
}

func (s *session) Begin() (driver.Tx, error) {
	return s.BeginTx(nil,
		driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelDefault)})
}

// ErrInvalidIsolationLevel is raised when an unsupported isolation level is asked.
var ErrInvalidIsolationLevel = errors.New("tds: invalid or unsupported isolation level")

// ErrNoReadOnly is raise when readonly attribute of driver.TxOptions is set.
// Readonly sessions are not supported by sybase.
var ErrNoReadOnly = errors.New("tds: readonly is unsupported")

// map between sql.IsolationLevel and sybase isolation levels
var isolationLevelMap = []int{isolationReadCommited, isolationReadUncommited,
	isolationReadCommited, isolationNotImplemented,
	isolationRepeatableRead, isolationNotImplemented, isolationSerializable,
	isolationNotImplemented}

// BeginTx implements driver.ConnBeginTx interface
func (s *session) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return s, ErrNoReadOnly
	}
	if sql.IsolationLevel(opts.Isolation) != sql.LevelDefault {
		if int(opts.Isolation) > len(isolationLevelMap) {
			return s, ErrInvalidIsolationLevel
		}
		level := isolationLevelMap[int(opts.Isolation)]

		if level == isolationNotImplemented {
			return s, ErrInvalidIsolationLevel
		}

		// send the option change command to set isolation level
		optCmd := optionCmd{msg: newMsg(OptionCmd), command: optionSet,
			option: optionIsolationLevel, value: level}

		if err := s.b.Send(ctx, netlib.Normal, &optCmd); err != nil {
			s.valid = false
			return s, s.checkErr(err, "tds: isolation level set failed", false)
		}

		_, err := s.processResponse(ctx, map[byte]netlib.Messager{}, false)
		if err = s.checkErr(err, "tds: isolation level set failed", true); err != nil {
			s.valid = false
			return s, err
		}
	}
	_, err := s.simpleExec(ctx, `begin tran
		if @@transtate != 0 raiserror 25000 'Invalid transaction state'`)
	return s, s.checkErr(err, "tds: begin failed", true)
}

func (s *session) Commit() error {
	_, err := s.simpleExec(nil, `if @@trancount > 0 commit tran
							if @@transtate != 1 raiserror 25000 'Invalid transaction state'`)
	return s.checkErr(err, "tds: commit failed", true)
}

func (s *session) Rollback() error {
	_, err := s.simpleExec(nil, `if @@trancount > 0 rollback tran
							if @@transtate != 3 raiserror 25000 'Invalid transaction state'`)
	return s.checkErr(err, "tds: rollback failed", true)
}

// Ping implements driver.Pinger interface
func (s *session) Ping(ctx context.Context) error {
	if !s.valid {
		return driver.ErrBadConn
	}

	value, err := s.SelectValue(ctx, "select 1")
	if err != nil {
		return err
	}

	intVal, ok := value.(int64)
	if !ok || intVal != 1 {
		return errors.New("sql: ping command failed. Invalid data returned")
	}

	return nil
}

// Exec implements the Execer interface.
// The aim is to use language queries when no parameters are given
func (s *session) Query(query string, args []driver.Value) (driver.Rows, error) {
	if len(args) != 0 {
		return nil, driver.ErrSkip
	}
	return s.simpleQuery(nil, query)
}

// Implement the "QueryerContext" interface
func (s *session) QueryContext(ctx context.Context, query string,
	namedArgs []driver.NamedValue) (driver.Rows, error) {
	if len(namedArgs) != 0 {
		return nil, driver.ErrSkip
	}
	return s.simpleQuery(ctx, query)
}

func (s *session) simpleQuery(ctx context.Context, query string) (rows *Rows, err error) {
	if !s.valid {
		return &emptyRows, driver.ErrBadConn
	}

	// send query
	if err := s.b.Send(ctx, netlib.Normal, &language{msg: newMsg(Language), query: query}); err != nil {
		s.valid = false
		return &emptyRows, s.checkErr(err, "tds: query send failed", false)
	}
	s.clearResult()

	return newRow(ctx, s)
}

// Exec implements the Querier interface.
// The aim is to use language queries when no parameters are given
func (s *session) Exec(query string, args []driver.Value) (driver.Result, error) {
	if len(args) != 0 {
		return &emptyResult, driver.ErrSkip
	}

	return s.simpleExec(nil, query)
}

// Implement the "ExecerContext" interface
func (s *session) ExecContext(ctx context.Context, query string,
	namedArgs []driver.NamedValue) (driver.Result, error) {
	if len(namedArgs) != 0 {
		return &emptyResult, driver.ErrSkip
	}

	return s.simpleExec(ctx, query)
}

func (s *session) simpleExec(ctx context.Context, query string) (res *Result, err error) {
	if !s.valid {
		return &emptyResult, driver.ErrBadConn
	}

	// send query
	rows, err := s.simpleQuery(ctx, query)
	if err = s.checkErr(err, "tds: exec failed", true); err != nil {
		return &emptyResult, err
	}

	rows.Close()
	return &(*s.res), nil
}

// Prepare prepares a statement and returns it
func (s *session) Prepare(query string) (driver.Stmt, error) {
	if !s.valid {
		return &emptyStmt, driver.ErrBadConn
	}
	return newStmt(nil, s, query)
}

// Prepare prepares a statement and returns it
func (s *session) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if !s.valid {
		return &emptyStmt, driver.ErrBadConn
	}
	return newStmt(ctx, s, query)
}

// Reads exactly one value from an sql query
func (s *session) SelectValue(ctx context.Context, query string) (value interface{}, err error) {
	// send query
	rows, err := s.simpleQuery(ctx, query)
	if err != nil {
		return nil, s.checkErr(err, "tds: select value failed", false)
	}
	defer rows.Close()

	vals := make([]driver.Value, 1)
	err = rows.Next(vals)
	if err != io.EOF && err != nil {
		return nil, err
	}
	return vals[0], nil
}

func (s *session) clearResult() {
	s.res = &Result{lastError: nil, s: s}
}

// process all common tokens (doneToken, doneInProc, envChange, info, etc)
//
// when a matching token is found in the messages map,
// it will read it from the wire and return only if doBreak is true.
// This is usefull to give the possibility for the caller to change a state
// before processing other messages from this stream.
func (s *session) processResponse(ctx context.Context,
	messages map[byte]netlib.Messager, doBreak bool) (byte, error) {

	return s.b.Receive(ctx,
		messages,
		func(t byte, doBreak bool) (bool, error) {
			var err error
			// process all common tokens (doneToken, doneInProc, envChange, info, etc)
			// this will fill the result structure, the sqlMessages array, etc
			switch token(t) {
			default:
				s.prev = token(t)
				if doBreak {
					return doBreak, nil
				}
			case SQLMessage:
				err = s.processsqlMessage()
			case EnvChange:
				err = s.processEnvChange()
			case ReturnStatus:
				err = s.processReturnStatus()
			case DoneProc, DoneInProc, Done:
				// last message for this stream
				err = s.processDone(token(t))
			}
			s.prev = token(t)

			// error was found, return now to caller.
			// Typically processDone returns an error
			// when a critical sybase error was faced during processing of the rows.
			// We need to make this error bubbles up.
			if err != nil {
				return true, err
			}
			return false, err
		},
		doBreak,
		func(t byte) netlib.Messager {
			return newMsg(token(t))
		})
}

// process the error/info messages and determine if there's an error
func (s *session) processsqlMessage() (err error) {
	// add it to the list of messages which is reset at each query
	s.res.messages = append(s.res.messages, s.sqlMessage.SybError)

	// propagate if its an error
	if s.IsError(s.sqlMessage.SybError) {
		s.res.lastError = s.sqlMessage.SybError
	}

	return nil
}

// process the env change messages and determine if there's an error
func (s *session) processEnvChange() (err error) {
	switch s.envChange.changeType {
	default:
		return fmt.Errorf("tds: unknow env change type: %#x",
			s.envChange.changeType)
	case dbChange:
		s.database = s.envChange.newValue
	case langChange:
		s.language = s.envChange.newValue
	case charsetChange:
		switch s.envChange.newValue {
		default:
			if err = s.b.SetCharset(s.envChange.newValue); err != nil {
				return fmt.Errorf("tds: cannot encode to '%s' charset", s.envChange.newValue)
			}
			s.charset = s.envChange.newValue
			s.charConvert = true
		case "":
			if err = s.b.SetCharset(s.envChange.oldValue); err != nil {
				return fmt.Errorf("tds: cannot encode to '%s' charset", s.envChange.oldValue)
			}
			s.charset = s.envChange.oldValue
			s.charConvert = true
		case "utf8":
			s.charset = s.envChange.newValue
		}
	case packetSizeChange:
		if packetSize, err := strconv.Atoi(s.envChange.newValue); err == nil {
			s.packetSize = packetSize
			s.b.PacketSize = packetSize
		}
	}
	return nil
}

// process the done token's information (row count, error status, final ?)
func (s *session) processDone(t token) (err error) {
	// ignore most doneInProc tokens
	if t == DoneInProc && !(s.prev == Row ||
		s.prev == CmpRow || s.prev == Done ||
		s.prev == ColumnFmt || s.prev == CmpRowFmt ||
		s.prev == WideColumnFmt) {
		return nil
	}

	// get row count if any
	if s.done.status&doneCount != 0 ||
		s.done.status&doneProcCount != 0 ||
		(s.done.status&doneProc != 0 && s.prev == Done) {
		// done with doneProc set will contain
		// the row count for inserts in prepared statements when "send doneinproc" is 0
		s.res.hasAffectedRows = true
		s.res.affectedRows = int64(s.done.count)
	}

	// check if this token indicates an error
	if s.done.status&doneError != 0 && s.res.lastError == nil {
		s.res.lastError = errors.New("unknow error reported by done token")
	}

	// last bit set
	s.res.final = s.done.status&doneMoreResults == 0

	// return error if found during this message stream.
	if s.res.final {
		if s.res.lastError != nil {
			return s.res.lastError
		}
		return io.EOF
	}
	return nil
}

// process the return status token. Sent when executing a stored procedure
func (s *session) processReturnStatus() (err error) {
	s.res.hasReturnStatus = true
	s.res.returnStatus = int(s.returnStatus.status)
	return nil
}
