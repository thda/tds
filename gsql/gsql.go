// test package
// not for production use
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"os/user"
	"regexp"
	"strings"
	"syscall"

	"github.com/thda/tds"
	"github.com/xo/tblfmt"

	"github.com/chzyer/readline"
)

var (
	version         = "0.01"
	noHeader        = false
	echoInput       = false
	noPSInInput     = false
	printVersion    = false
	chained         = false
	packetSize      = 512
	terminator      = ";|^go"
	database        = "master"
	hostname        string
	inputFile       string
	charset         string
	loginTimeout    = 60
	outputFile      string
	password        string
	columnSeparator = " "
	server          string
	commandTimeout  = 0
	pageSize        = 3000
	userName        string
	locale          string
	width           int
	ssl             = "off"
	theme           = "UtfCompact"
	outFormat       = "table"
	re              *regexp.Regexp
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: example -stderrthreshold=[INFO|WARN|FATAL] -log_dir=[string]\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func init() {
	hostname, _ = os.Hostname()
	flag.Usage = usage
	flag.BoolVar(&noHeader, "b", false, "disable headers")
	flag.BoolVar(&echoInput, "e", false, "print commands before execution")
	flag.BoolVar(&noPSInInput, "n", false, "no prompt is printed when displaying commands")
	flag.BoolVar(&printVersion, "v", false, "print version and exit")
	flag.BoolVar(&chained, "Y", false, "use chained mode. Can break lots of stored procedures")
	flag.IntVar(&packetSize, "A", 0, "custom network packet size. Zero to let the server handle it.")
	flag.StringVar(&terminator, "c", ";|^go", "the terminator used to determine the end of a command. Can contain regex.")
	flag.StringVar(&database, "D", database, "database to use.")
	flag.StringVar(&hostname, "H", "system hostname", "client's host name to send to the server.")
	flag.StringVar(&inputFile, "i", "/gsqlnone/", "file to read commands from")
	flag.StringVar(&charset, "J", charset, "character set")
	flag.StringVar(&theme, "T", theme, "display theme, can be ASCIICompact or UtfCompact")
	flag.IntVar(&loginTimeout, "l", 0, "login Timeout")
	flag.StringVar(&outputFile, "o", "/gsqlnone/", "file to output to")
	flag.StringVar(&password, "P", "none", "password")
	flag.IntVar(&pageSize, "p", pageSize, "paging size")
	flag.StringVar(&columnSeparator, "s", columnSeparator, "column separator")
	flag.StringVar(&server, "S", " ", "host:port")
	flag.IntVar(&commandTimeout, "t", 0, "command Timeout")
	flag.IntVar(&width, "w", 0, "line width")
	flag.StringVar(&userName, "U", "none", "user name")
	flag.StringVar(&ssl, "x", ssl, "Set to 'on' to enable ssl")
	flag.StringVar(&locale, "z", "none", "locale name")
	flag.StringVar(&outFormat, "f", outFormat, "output format. Can be 'table','json', or 'csv'")
	flag.Parse()

	re = regexp.MustCompile("(" + terminator + ")\n?$")

	// check for mandatory parameters
	if userName == "" || server == "" {
		fmt.Fprintf(os.Stderr, "usage: example -stderrthreshold=[INFO|WARN|FATAL] -log_dir=[string]\n")
		flag.PrintDefaults()
		os.Exit(1)
	}
}

// build the connection string
func buildCnxStr() string {
	// build the url
	v := url.Values{}
	if chained {
		v.Set("mode", "chained")
	}
	if packetSize != 0 {
		v.Set("packetSize", fmt.Sprintf("%d", packetSize))
	}
	if ssl == "on" {
		v.Set("ssl", "on")
	}
	v.Set("hostname", hostname)
	v.Set("readTimeout", "10")
	if charset != "" {
		v.Set("charset", charset)
	}
	return "tds://" + url.QueryEscape(userName) + ":" + url.QueryEscape(password) +
		"@" + server + "/" + url.QueryEscape(database) + "?" + v.Encode()
}

// find the string terminator in a line and add it to the current batch if needed
func processLine(terminator string, line string, batch string) (batchOut string, found bool) {
	// continue till we get a the terminator
	if match := re.MatchString(line); !match {
		if batch == "" {
			batchOut = line
		} else {
			// add the line to the batch
			batchOut = batch + "\n" + line
		}
		return batchOut, false
	}
	return batch + re.ReplaceAllString(line, ""), true
}

type SQLBatchReader interface {
	ReadBatch(terminator string) (batch string, err error)
	Close() error
}

type fileBatchReader struct {
	io.ReadCloser
	scanner *bufio.Reader
	w       *bufio.Writer
}

// get an instance of readline with the proper settings
func newFileBatchReader(inputFile string, w *bufio.Writer) (r *fileBatchReader, err error) {
	r = &fileBatchReader{w: w}
	if inputFile == "-" {
		r.ReadCloser = os.Stdin
	} else if r.ReadCloser, err = os.Open(inputFile); err != nil {
		return nil, err
	}
	r.scanner = bufio.NewReader(r.ReadCloser)
	return r, nil
}

func (r *fileBatchReader) ReadBatch(terminator string) (batch string, err error) {
	found := false
	lineNo := 1
	batch = ""
	for {
		line, err := r.scanner.ReadString('\n')
		if err != nil && (err != io.EOF || line == "") {
			return batch, err
		}
		batch, found = processLine(terminator, line, batch)

		if echoInput {
			fmt.Fprintf(r.w, "%d> %s", lineNo, line)
		}
		lineNo++

		// found the separator
		if found {
			lineNo = 1
			return batch, nil
		}

	}
}

type readLineBatchReader struct {
	*readline.Instance
	server string
	conn   *sql.DB
}

func (r *readLineBatchReader) ReadBatch(terminator string) (batch string, err error) {
	found := false
	lineNo := 1
	for {
		var prompt string
		row := r.conn.QueryRow("select @@servername")
		if err == nil {
			row.Scan(&r.server)
		}

		prompt = fmt.Sprintf("%d $ ", lineNo)
		if r.server != "" {
			prompt = fmt.Sprintf("%s %d $ ", r.server, lineNo)
		}

		r.SetPrompt(prompt)
		line, err := r.Readline()

		if err == readline.ErrInterrupt {
			lineNo = 1
			batch = ""
			continue
		}
		if err != nil {
			return "", err
		}

		batch, found = processLine(terminator, line, batch)
		if found {
			lineNo = 1
			r.SaveHistory(batch)
			return batch, nil
		}
		lineNo++
	}
}

// get an instance of readline with the proper settings
func newReadLineBatchReader(conn *sql.DB) (SQLBatchReader, error) {
	usr, _ := user.Current()
	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 "$ ",
		HistoryFile:            usr.HomeDir + "/.gsql_history.txt",
		DisableAutoSaveHistory: true,
	})

	rl.SetPrompt("1> ")

	if err != nil {
		return nil, fmt.Errorf("newReadLine: error while initiating readline object (%s)", err)
	}

	return &readLineBatchReader{Instance: rl, conn: conn}, err
}

// Result is the struct to json encode
type Result struct {
	Messages     string          `json:"messages"`
	Results      json.RawMessage `json:"results"`
	Duration     uint64          `json:"duration"`
	Error        string          `json:"error"`
	ReturnStatus int             `json:"return_status"`
}

func main() {
	// defer profile.Start(profile.CPUProfile).Stop()
	var batch string
	var r SQLBatchReader
	var w *bufio.Writer
	var mb strings.Builder // message buffer

	mo := bufio.NewWriter(os.Stdout)
	if outFormat == "json" {
		mo = bufio.NewWriter(&mb)
	}

	// connect
	conn, err := sql.Open("tds", buildCnxStr())
	if err != nil {
		fmt.Println("failed to connect: ", err)
		os.Exit(1)
	}

	// print showplan messages and all
	conn.Driver().(tds.ErrorHandler).SetErrorhandler(func(m tds.SybError) bool {
		if m.Severity == 10 {
			if (m.MsgNumber >= 3612 && m.MsgNumber <= 3615) ||
				(m.MsgNumber >= 6201 && m.MsgNumber <= 6299) ||
				(m.MsgNumber >= 10201 && m.MsgNumber <= 10299) {
				mo.WriteString(m.Message)
			} else {
				mo.WriteString(strings.TrimRight(m.Message, "\n"))
			}
		}

		if m.Severity > 10 {
			mo.WriteString(m.Error())
		}
		return m.Severity > 10
	})

	// open outpout
	switch outputFile {
	default:
		var f io.WriteCloser
		var _, err = os.Stat(outputFile)
		if os.IsNotExist(err) {
			// not found...
			f, err = os.Create(outputFile)
		} else {
			// truncate the file if it exists
			if err = os.Truncate(outputFile, 0); err == nil {
				f, err = os.OpenFile(outputFile, os.O_WRONLY, os.ModePerm)
			}
		}
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		defer f.Close()
		w = bufio.NewWriter(f)
		defer w.Flush()

	case "/gsqlnone/":
		w = bufio.NewWriter(os.Stdout)
		defer w.Flush()
	}

	// open input
	switch inputFile {
	case "/gsqlnone/":
		// get readline instance
		r, err = newReadLineBatchReader(conn)

	default:
		r, err = newFileBatchReader(inputFile, w)
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer r.Close()

input:
	for {
		batch, err = r.ReadBatch(terminator)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}

		// handle cancelation
		ctx, cancel := context.WithCancel(context.Background())

		c := make(chan os.Signal)
		done := make(chan struct{})
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			select {
			case <-c:
				cancel()
				<-done
			case <-done:
			}
		}()

		// send query
		rows, err := conn.QueryContext(ctx, batch)
		// flush message buffer
		mo.Flush()
		select {
		case <-done:
		case done <- struct{}{}:
		}

		switch outFormat {
		case "json":
			if err != nil {
				out, _ := json.Marshal(&Result{
					Messages: mb.String(), ReturnStatus: 1, Error: err.Error()})
				fmt.Fprintln(w, string(out))
				mb = strings.Builder{}
				continue input
			}
			for {
				rb := strings.Builder{}
				tblfmt.EncodeJSON(&rb, rows)
				results := rb.String()
				if results == "" {
					results = "{}"
				}
				out, _ := json.Marshal(&Result{Results: json.RawMessage(results),
					Messages: mb.String()})
				fmt.Fprintln(w, string(out))
				if !rows.NextResultSet() {
					mb = strings.Builder{}
					continue input
				}
			}
		default:
			if err != nil {
				// SQL errors are printed by the error handler
				if _, ok := err.(tds.SybError); !ok {
					fmt.Println(err)
				}
				continue input
			}
			tblfmt.EncodeAll(w, rows, map[string]string{"format": "aligned", "border": "2",
				"unicode_border_linestyle": "single", "linestyle": "unicode"})
		}
		rows.Close()
	}
}
