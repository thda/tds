package tds

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
)

const defaultCharset = "utf8"
const defaultTextSize = 32768

// connection Timeout in seconds
const defaultLoginTimeout = 20

type connParams struct {
	host         string
	user         string
	password     string
	clientHost   string // client host name
	app          string // client application name
	charset      string
	packetSize   int
	loginTimeout int    // login Timeout
	readTimeout  int    // read Timeout
	writeTimeout int    // write Timeout
	database     string // if requested at connection time
	pid          string
	textSize     int
	ssl          string
	// yes: mandatory password encryption.
	// no: never encrypt password.
	// try: try encryption, fallback to non encrypted password.
	encryptPassword string
}

// Conn encapsulates a tds session and satisties driver.Connc
type Conn struct {
	*session
}

// parse the DSN given by the user
func parseDSN(dsn string) (prm connParams, err error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return prm, err
	}

	// get server / database
	prm.host = url.Host
	if len(url.Path) > 1 {
		prm.database = url.Path[1:len(url.Path)]
	}

	// user/pass
	if url.User != nil {
		prm.user = url.User.Username()
		prm.password, _ = url.User.Password()
	}

	// additionnal parameters
	values := url.Query()
	prm.packetSize, _ = strconv.Atoi(values.Get("packetSize"))
	if prm.packetSize == 0 {
		prm.packetSize = 512
	}

	// get login, read and write Timeouts
	prm.loginTimeout, err = strconv.Atoi(values.Get("loginTimeout"))
	if err != nil || prm.loginTimeout <= 0 {
		prm.loginTimeout = defaultLoginTimeout
	}

	prm.readTimeout, err = strconv.Atoi(values.Get("readTimeout"))
	prm.writeTimeout, err = strconv.Atoi(values.Get("writeTimeout"))

	// get password encryption method
	prm.encryptPassword = values.Get("encryptPassword")
	if prm.encryptPassword == "" {
		prm.encryptPassword = "try"
	}

	if prm.encryptPassword != "yes" &&
		prm.encryptPassword != "no" &&
		prm.encryptPassword != "try" {
		return prm, fmt.Errorf("tds: encryptPassword must be 'yes', 'no' or 'try'")
	}

	// ssl ??
	if values.Get("ssl") == "on" {
		prm.ssl = "on"
	}

	switch values.Get("charset") {
	case "none":
		prm.charset = ""
	case "utf8", "utf-8", "UTF8", "UTF-8", "":
		prm.charset = "utf8"
	default:
		prm.charset = values.Get("charset")
	}

	prm.app = values.Get("applicationName")
	prm.clientHost = values.Get("hostName")
	prm.pid = values.Get("pid")
	prm.textSize, _ = strconv.Atoi(values.Get("textSize"))
	if err != nil {
		prm.textSize = defaultTextSize
	}

	// mandatory parameters
	if prm.host == "" {
		return prm, errors.New("tds: connect failed. Please specify hostname")
	}
	if prm.user == "" {
		return prm, errors.New("tds: connect failed. Please specify user")
	}
	if validHost.FindString(prm.host) == "" {
		return prm, errors.New("tds: connect failed. Please specify host name in the form host:port")
	}

	if prm.packetSize != 512 && prm.packetSize != 1024 &&
		prm.packetSize != 2048 && prm.packetSize != 4096 {
		return prm, errors.New("tds: invalid packet size. must be 512, 1024, 2048 or 4096")
	}

	return prm, nil
}

// SetErrorhandler allows setting a custom error handler.
// The function shall accept an SQL Message and return a boolean
// indicating if this message is indeed a critical error.
func (c *Conn) SetErrorhandler(fn func(s SybError) bool) {
	c.IsError = fn
}

// NewConn returns a TDS session
func NewConn(dsn string) (*Conn, error) {
	prm, err := parseDSN(dsn)

	if err != nil {
		return &emptyConn, err
	}
	s, err := newSession(prm)
	c := &Conn{session: s}
	return c, err
}

// GetEnv return a map of environments variables.
// The following keys are garanteed to be present:
//  - server
//  - database
//  - charset
func (c Conn) GetEnv() map[string]string {
	return map[string]string{
		"server":     c.session.serverType,
		"serverType": c.session.serverType,
		"database":   c.session.database,
		"charset":    c.session.charset,
	}
}

// ErrorHandler is a connection which support defines sybase error handling
type ErrorHandler interface {
	SetErrorhandler(fn func(s SybError) bool)
}

// SybDriver is the driver implementing driver.Driver interface
type SybDriver struct {
	sync.Mutex
	IsError func(s SybError) bool
}

var sybDriverInstance = &SybDriver{}

// Open opens a connection to the server.
// See https://github.com/thda/tds#connection-string for the dsn formatting.
// It also set the custum error handler if any.
func (d *SybDriver) Open(dsn string) (driver.Conn, error) {
	conn, err := NewConn(dsn)
	if d.IsError != nil {
		conn.SetErrorhandler(d.IsError)
	}
	return conn, err
}

// SetErrorhandler allows setting a custom error handler.
// The function shall accept an SQL Message and return a boolean
// indicating if this message is indeed a critical error.
//
// Example:
//
// 	// Print showplan messages
// 	conn.Driver().(tds.ErrorHandler).SetErrorhandler(func(m tds.SybError) bool {
// 		if m.Severity == 10 {
// 			if (m.MsgNumber >= 3612 && m.MsgNumber <= 3615) ||
// 				(m.MsgNumber >= 6201 && m.MsgNumber <= 6299) ||
// 				(m.MsgNumber >= 10201 && m.MsgNumber <= 10299) {
// 				fmt.Printf(m.Message)
// 			} else {
// 				fmt.Println(strings.TrimRight(m.Message, "\n"))
// 			}
// 		}
//
// 		if m.Severity > 10 {
// 			fmt.Print(m)
// 		}
// 		return m.Severity > 10
// 	})
func (d *SybDriver) SetErrorhandler(fn func(s SybError) bool) {
	d.Lock()
	defer d.Unlock()
	d.IsError = fn
}

func init() {
	sql.Register("syb", sybDriverInstance)
	sql.Register("tds", sybDriverInstance)
}

var _ driver.Driver = (*SybDriver)(nil)

// empty objects to return on error
// Make sure the session is not nil to avoid nil pointers
var emptySession = session{}
var emptyConn = Conn{session: &emptySession}
var emptyRows = Rows{s: &emptySession}
var emptyResult = Result{s: &emptySession}
var emptyStmt = Stmt{s: &emptySession}
