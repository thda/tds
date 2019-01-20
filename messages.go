package tds

import (
	"fmt"
	"os"
	"reflect"
	"strconv"

	bin "github.com/thda/tds/binary"

	"errors"
)

//go:generate stringer -type=token
type token byte

// message Tokens
const (
	None          token = 0x00
	CapabilityReq token = 0x01
	CapabilityRes token = 0x02
	ParamFmt2     token = 0x20 // 32
	Language      token = 0x21 // 33
	OrderBy2      token = 0x22 // 34
	WideColumnFmt token = 0x61 // 97
	Dynamic2      token = 0x62 // 98
	Msg           token = 0x65 // 101
	ReturnStatus  token = 0x79 // 121
	CurClose      token = 0x80 // 128
	CurDelete     token = 0x81 // 129
	CurFetch      token = 0x82 // 130
	CurFmt        token = 0x83 // 131
	CurOpen       token = 0x84 // 132
	CurDeclare    token = 0x86 // 134
	Logout        token = 0x71 // 113
	TableName     token = 0xa4 // 164
	ColumnInfo    token = 0xa5 // 165
	OptionCmd     token = 0xa6 // 166
	CmpRowName    token = 0xa7 // 167
	CmpRowFmt     token = 0xa8 // 168
	OrderBy       token = 0xa9 // 169
	Info          token = 0xab // 171
	LoginAck      token = 0xad // 173
	Control       token = 0xae // 174
	Row           token = 0xd1 // 209
	CmpRow        token = 0xd3 // 211
	Param         token = 0xd7 // 215
	Capabilities  token = 0xe2 // 226
	EnvChange     token = 0xe3 // 227
	SQLMessage    token = 0xe5 // 229
	DbRPC         token = 0xe6 // 230
	Dynamic       token = 0xe7 // 231
	ParamFmt      token = 0xec // 236
	Auth          token = 0xed // 237
	ColumnFmt     token = 0xee // 238
	Done          token = 0xfd // 253
	DoneProc      token = 0xfe // 254
	DoneInProc    token = 0xff // 255
)

// Message attribute options
const (
	noFlag          = 0
	fixedSize uint8 = 1 << iota // fixed length message ?
	// shall we use a limited reader for this messages ?
	//
	// This is used by some messages
	// when there is now way to tell we are at the end of the messages.
	// See for example columnInfo, tabName.
	limitRead
	long  // 32 bits length field
	short // 8 bits length field
	ignoreSize
)

// msg is the struct containing all the attributes of a TDS message.
// It is embeded in all message structs and implements the netlib.Message interface.
type msg struct {
	token token
	flags uint8
	size  uint8 // Size for fixed length messages
}

func (attr msg) Token() byte {
	return byte(attr.token)
}

func (attr msg) Size() uint8 {
	if attr.flags&fixedSize != 0 {
		return attr.size
	}
	return 0
}

func (attr msg) SizeLen() uint8 {
	if attr.flags&short != 0 {
		return 8
	}
	if attr.flags&long != 0 {
		return 32
	}
	if attr.flags&fixedSize != 0 || attr.flags&ignoreSize != 0 {
		return 0
	}
	return 16
}

func (attr msg) LimitRead() bool {
	return attr.flags&limitRead != 0
}

func (attr msg) Write(*bin.Encoder) error {
	return nil
}

func (attr msg) Read(*bin.Encoder) error {
	return nil
}

// msgs is a table which contains the attributes for each message type
var msgs = map[token]msg{
	None:          {None, noFlag, 0},
	CapabilityReq: {CapabilityReq, noFlag, 0},
	CapabilityRes: {CapabilityRes, noFlag, 0},
	ParamFmt2:     {ParamFmt2, long, 0},
	Language:      {Language, ignoreSize, 0},
	OrderBy2:      {OrderBy2, long, 0},
	CmpRowFmt:     {CmpRowFmt, noFlag, 0},
	CmpRowName:    {CmpRowName, noFlag, 0},
	WideColumnFmt: {WideColumnFmt, long, 0},
	Dynamic2:      {Dynamic2, long, 0},
	ReturnStatus:  {ReturnStatus, fixedSize, 4},
	CurClose:      {CurClose, noFlag, 0},
	CurDelete:     {CurDelete, noFlag, 0},
	CurFetch:      {CurFetch, noFlag, 0},
	CurFmt:        {CurFmt, noFlag, 0},
	CurOpen:       {CurOpen, noFlag, 0},
	CurDeclare:    {CurDeclare, noFlag, 0},
	Logout:        {Logout, fixedSize, 1},
	TableName:     {TableName, limitRead, 0},
	ColumnInfo:    {ColumnInfo, limitRead, 0},
	OptionCmd:     {OptionCmd, noFlag, 0},
	Control:       {Control, noFlag, 0},
	OrderBy:       {OrderBy, noFlag, 0},
	LoginAck:      {LoginAck, noFlag, 0},
	Row:           {Row, ignoreSize, 0},
	Param:         {Param, ignoreSize, 0},
	CmpRow:        {CmpRow, ignoreSize, 0},
	Capabilities:  {Capabilities, noFlag, 0},
	EnvChange:     {EnvChange, noFlag, 0},
	SQLMessage:    {SQLMessage, noFlag, 0},
	Info:          {Info, noFlag, 0},
	DbRPC:         {DbRPC, noFlag, 0},
	Dynamic:       {Dynamic, noFlag, 0},
	ParamFmt:      {ParamFmt, noFlag, 0},
	ColumnFmt:     {ColumnFmt, noFlag, 0},
	Done:          {Done, fixedSize, 8},
	Msg:           {Msg, short, 0},
	DoneProc:      {DoneProc, fixedSize, 8},
	DoneInProc:    {DoneInProc, fixedSize, 8}}

func newMsg(t token) msg {
	// check for existence
	attr, _ := safeGetMsg(t)
	return attr
}

func safeGetMsg(t token) (msg, error) {
	// check for existence
	attr, ok := msgs[t]
	if !ok {
		return msg{}, fmt.Errorf("tds: unknown token %s", t)
	}
	return attr, nil
}

//
// capabilities
//
const defaultcapabilitiesLength = 14

// capabilities request bit
const (
	_ = iota
	reqLang
	reqRPC
	reqEvt
	reqMstmt
	reqBcp
	reqCursor
	reqDynf
	reqMsg
	reqParam
	dataInt1
	dataInt2
	dataInt4
	dataBit
	dataChar
	dataVchar
	dataBin
	dataVbin
	dataMny8
	dataMny4
	dataDate8
	dataDate4
	dataFlt4
	dataFlt8
	dataNum
	dataText
	dataImage
	dataDec
	dataLchar
	dataLbin
	dataIntn
	dataDatetimen
	dataMoneyn
	csrPrev
	csrFirst
	csrLast
	csrAbs
	csrRel
	csrMulti
	conOob
	conInband
	conLogical
	protoText
	protoBulk
	reqUrgevt
	dataSensitivity
	dataBoundary
	protoDynamic
	protoDynproc
	dataFltn
	dataBitn
	dataInt8
	dataVoid
	dolBulk
	objectJava1
	objectChar
	reqReserved1
	objectBinary
	dataColumnstatus
	widetable
	reqReserved2
	dataUint2
	dataUint4
	dataUint8
	dataUintn
	curImplicit
	dataNlbin
	imageNchar
	blobNchar16
	blobNchar8
	blobNcharScsu
	dataDate
	dataTime
	dataInterval
	csrScroll
	csrSensitive
	csrInsensitive
	csrSemisensitive
	csrKeysetdriven
	reqSrvpktsize
	dataUnitext
	capClusterfailover
	dataSint1
	reqLargeident
	reqBlobNchar16
	dataXML
	reqCurinfo3
	reqDbrpc2
	_
	reqMigrate
	multiRequests
	_
	_
	dataBigdatetime
	dataUsecs
	rpcparamLob
	reqInstid
	reqGrid
	reqDynBatch
	reqLangBatch
	reqRPCBatch
	dataLoblocator
	reqRowCountSelect
	reqLogParams
	reqDynNoParamFmt
	reqRO
)

// capabilities response bits
const (
	_ = iota
	resNomsg
	resNoeed
	resNoparam
	dataNoint1
	dataNoint2
	dataNoint4
	dataNobit
	dataNochar
	dataNovchar
	dataNobin
	dataNovbin
	dataNomny8
	dataNomny4
	dataNodate8
	dataNodate4
	dataNoflt4
	dataNoflt8
	dataNonum
	dataNotext
	dataNoimage
	dataNodec
	dataNolchar
	dataNolbin
	dataNointn
	dataNodatetimen
	dataNomoneyn
	conNooob
	conNoinband
	protoNotext
	protoNobulk
	dataNosensitivity
	dataNoboundary
	resNotdsdebug
	resNostripblanks
	dataNoint8
	objectNojava1
	objectNochar
	dataNocolumnstatus
	objectNobinary
	resReserved1
	dataNouint2
	dataNouint4
	dataNouint8
	dataNouintn
	noWidetables
	dataNonlbin
	imageNonchar
	blobNonchar16
	blobNonchar8
	blobNoncharScsu
	dataNodate
	dataNotime
	dataNointerval
	dataNounitext
	dataNosint1
	resNolargeident
	resNoblobNchar16
	noSrvpktsize
	resNodataXML
	nonintReturnValue
	resNoxnldata
	resSuppressFmt
	resSuppressDoneinproc
	resForceRowfmt2
	dataNobigdatetime
	dataNousecs
	resNoTdscontrol
	rpcparamNolob
	_
	dataNoloblocator
)

var defaultReqcapabilities = [...]int{dataLoblocator, reqLangBatch, reqDynBatch,
	rpcparamLob, dataUsecs, dataBigdatetime,
	reqDbrpc2, reqCurinfo3, dataXML, reqLargeident, dataUnitext,
	reqSrvpktsize, csrSemisensitive, csrInsensitive, csrScroll, dataTime,
	dataDate, dataNlbin, curImplicit, dataUintn,
	dataUint8, dataUint4, dataUint2, widetable,
	dolBulk, dataVoid, dataInt8, dataFltn, protoDynproc,
	dataBoundary, dataSensitivity, conInband,
	csrMulti, csrRel, csrAbs, csrLast, csrFirst, csrPrev, dataMoneyn,
	dataDatetimen, dataIntn, dataLbin, dataLchar, dataDec, dataImage, dataText, dataNum,
	dataFlt8, dataFlt4, dataDate4, dataDate8, dataMny4, dataMny8, dataVbin, dataBin,
	dataVchar, dataChar, dataBit, dataInt4, dataInt2, dataInt1, reqParam, reqMsg,
	reqDynf, reqCursor, reqBcp, reqRPC, reqLang}

var defaultRescapabilities = [...]int{resNoTdscontrol, resSuppressFmt,
	resNoxnldata, resNotdsdebug, objectNojava1, resSuppressDoneinproc}

// capabilities token. Tricky one. Stores the connection's capability
// in an array if bits.
type capabilities struct {
	msg
	reqToken    byte
	e           int8
	req         []byte
	resToken    byte
	resFieldLen int8
	res         []byte
}

func newCapabilities() *capabilities {
	c := &capabilities{reqToken: byte(CapabilityReq), resToken: byte(CapabilityRes)}
	c.req = make([]byte, defaultcapabilitiesLength)
	c.res = make([]byte, defaultcapabilitiesLength)

	// set capabilities from default
	for _, capability := range &defaultReqcapabilities {
		c.setcapabilities(CapabilityReq, capability)
	}
	for _, capability := range &defaultRescapabilities {
		c.setcapabilities(CapabilityRes, capability)
	}
	return c
}

// Setcapabilities sets the capabilities of a capability structs by playing with the bitmaps
func (c *capabilities) setcapabilities(capabilityType token, capabilities ...int) error {
	var target []byte
	var length int

	// determine the target array
	switch capabilityType {
	case CapabilityReq:
		target = c.req[:]
	case CapabilityRes:
		target = c.res[:]
	default:
		return errors.New("tds: invalid capability type. Should be capabilityReqToken or capabilityResToken")
	}

	length = len(target)

	for _, capability := range capabilities[:] {
		// get the byte to modify and the bit position in this byte
		capIndex := length - 1 - capability/8
		pos := uint(capability) % 8

		if capIndex >= length {
			return fmt.Errorf("tds: trying to write above the capacity array length, %d > %d", length, capIndex)
		}

		// bit shifting at the correct offset
		target[capIndex] |= (1 << pos)
	}
	return nil
}

// IsSet check if a capability is set
func (c *capabilities) isSet(capabilityType token, capability int) bool {
	var target []byte
	var length int

	switch capabilityType {
	case CapabilityReq:
		target = c.req[:]
	case CapabilityRes:
		target = c.res[:]
	default:
		return false
	}

	length = len(target)

	// get the byte to access and the bit position in this byte
	capIndex := length - 1 - capability/8
	pos := capability % 8

	// bit shifting at the correct offset
	return capIndex < length && target[capIndex]&(1<<uint(pos)) != 0
}

// Write serializes a capabilities struct
func (c capabilities) Write(e *bin.Encoder) error {
	e.WriteByte(c.reqToken)
	e.WriteInt8(int8(len(c.req)))
	e.Write(c.req[:])
	e.WriteByte(c.resToken)
	e.WriteInt8(int8(len(c.res)))
	e.Write(c.res[:])
	err := e.Err()
	return err
}

// Reads a capabilities struct
func (c *capabilities) Read(e *bin.Encoder) error {
	c.reqToken = e.ReadByte()
	c.e = e.Int8()
	c.req = make([]byte, c.e)
	e.Read(c.req[:])
	c.resToken = e.ReadByte()
	c.resFieldLen = e.Int8()
	c.res = make([]byte, c.resFieldLen)
	e.Read(c.res[:])
	err := e.Err()
	return err
}

//
// done
//

// Transaction states
const (
	doneNoTran       = iota // No transaction in effect
	doneTranSucceed         // Transaction completed successfully
	doneTranProgress        // Transaction in progress
	doneTranAbort           // Transaction aborted
)

// Done packets status
const (
	doneFinal       = 0x0000
	doneMoreResults = 0x0001
	doneError       = 0x0002
	doneCount       = 0x0010
	doneProcCount   = 0x0040
	doneCancel      = 0x0020
)

// done token
type done struct {
	msg
	status    int16
	tranState int16
	count     int32
}

func (d done) Write(e *bin.Encoder) error {
	e.WriteInt16(d.status)
	e.WriteInt16(d.tranState)
	e.WriteInt32(d.count)
	err := e.Err()
	return err
}

func (d *done) Read(e *bin.Encoder) error {
	d.status = e.Int16()
	d.tranState = e.Int16()
	d.count = e.Int32()
	err := e.Err()
	return err
}

// dynamic type
const (
	dynamicPrepare = 0x01 << iota
	dynamicExec
	dynamicDealloc
	dynamicExecImmediate
	dynamicProcname
	dynamicAck
	dynamicDescIn
	dynamicDescOut
)

// dynamic status
const (
	dynamicHasArgs = 0x01 << iota
	dynamicSuppressParamFmt
	dynamicBatchParams
)

//
// dynamic
//

// dynamic is a dynamic statement prepare / ack token
type dynamic struct {
	msg
	operation byte
	status    byte
	name      string // 1 byte length
	statement string // 4 bytes length
}

// Read unserializes a Dynamic2 struct
func (d *dynamic) Read(e *bin.Encoder) error {
	d.operation = e.ReadByte()
	d.status = e.ReadByte()

	d.name, _ = e.ReadString(8)

	// dynamicPrepare messages contain the statement
	if d.operation&dynamicPrepare > 0 {
		d.statement, _ = e.ReadString(32)
	}
	err := e.Err()
	return err
}

// Write writes the dynamic token to the wire
func (d dynamic) Write(e *bin.Encoder) error {
	e.WriteByte(d.operation)
	e.WriteByte(d.status)
	e.WriteStringWithLen(8, d.name)

	// DynamicPrepare messages contain the statement
	if d.operation&dynamicPrepare > 0 {
		e.WriteStringWithLen(32, d.statement)
	}
	err := e.Err()
	return err
}

//
// envChange
//

//go:generate stringer -type=changeType
type changeType byte

// login change tokens
const (
	dbChange         changeType = 0x01
	langChange       changeType = 0x02
	charsetChange    changeType = 0x03
	packetSizeChange changeType = 0x04
)

// EnvChange token used to change packet size, db, and some other options
type envChange struct {
	msg
	changeType changeType
	newValue   string // 8 bit length
	oldValue   string // 8 bit length
}

func (env envChange) Write(e *bin.Encoder) error {
	e.WriteByte(byte(env.changeType))
	e.WriteStringWithLen(8, env.newValue)
	e.WriteStringWithLen(8, env.oldValue)
	err := e.Err()
	return err
}

func (env *envChange) Read(e *bin.Encoder) error {
	env.changeType = changeType(e.ReadByte())
	env.newValue, _ = e.ReadString(8)
	env.oldValue, _ = e.ReadString(8)
	err := e.Err()
	return err
}

func (env envChange) String() string {
	return "env change: " + fmt.Sprint(env.changeType) +
		"\nold value: " + env.oldValue + "\nnew value: " + env.newValue
}

//
// language
//

// language token
type language struct {
	msg
	messageLen uint32
	status     byte
	query      string
}

// Read unserializes a TdsLanguage struct
func (l *language) Read(e *bin.Encoder) error {
	l.messageLen = e.Uint32()
	l.status = e.ReadByte()
	l.query = e.ReadStringWithLen(int(l.messageLen - 1))
	err := e.Err()
	return err
}

// Read unserializes a TdsLanguage struct
func (l language) Write(e *bin.Encoder) error {
	e.WriteUint32(uint32(len(l.query)) + 1)
	e.WriteByte(l.status)
	e.WriteString(l.query)
	err := e.Err()
	return err
}

// Implement the string interface for debugging purposes
func (l language) String() string {
	return fmt.Sprintf("status: %#x\n", l.status) +
		fmt.Sprintf("query: %s\n", l.query)
}

//
// login
//

const defaultLibrary = "gtds"

var defaultProtocolVersion = [4]byte{5, 0, 0, 0}
var defaultLibraryVersion = [4]byte{1, 0, 0, 0}

var (
	loginSecEncrypt1 = uint8(1)
	loginSecEncrypt2 = uint8(32)
	loginSecNonce    = uint8(128)
)

// login is the tds v5 login packet
type login struct {
	msg
	clientHost      string // 30 bytes
	user            string // 30 bytes
	password        string // 30 bytes
	pid             string // 30 bytes
	int2BE          int8
	int4BE          int8
	char            int8
	flt             int8
	dateBE          int8
	notifyDBChange  int8
	bulkCopy        int8
	_               [9]byte
	app             string // 30 bytes
	server          string // 30 bytes
	password2Length int
	password2       string // 253 bytes, length prefix and suffix
	protocolVersion [4]byte
	library         string // 30 bytes
	libraryVersion  [4]byte
	convertShort    int8
	float4BE        int8
	dateTime4BE     int8
	language        string // 30 bytes
	_               byte
	oldSecure       int16
	encrypted       uint8
	_               byte
	secSpare        [9]byte
	charset         string // 30 bytes
	_               int8
	packetSize      int // 6 bytes string
	capabilities    capabilities
}

// set the login capabilities and trim them
func (l *login) setCapabilities(c capabilities) {
	l.capabilities = c
}

func writeFixedSizeString(e *bin.Encoder, s string, size int, putSize bool) int {
	if len(s) > size {
		s = s[:size]
	}
	written := e.WriteString(s)
	e.Pad(0x00, size-written)
	if putSize {
		e.WriteInt8(int8(written))
	}
	return written
}

func readFixedSizeString(e *bin.Encoder, size int) string {
	out := make([]byte, size)
	e.Read(out)
	realSize := e.Int8()
	return string(out[:realSize])
}

func (l login) Write(e *bin.Encoder) error {
	writeFixedSizeString(e, l.clientHost, 30, true)
	writeFixedSizeString(e, l.user, 30, true)
	writeFixedSizeString(e, l.password, 30, true)
	writeFixedSizeString(e, fmt.Sprintf("%d", os.Getpid()), 30, true)
	e.WriteInt8(3)  // type of int2
	e.WriteInt8(1)  // type of int4
	e.WriteInt8(6)  // type of char
	e.WriteInt8(10) // type of flt
	e.WriteInt8(9)  // type of date
	e.WriteInt8(1)  // notify of use db
	e.WriteInt8(0)  // disallow dump/load and bulk insert
	e.Pad(0x00, 9)  // magic
	writeFixedSizeString(e, l.app, 30, true)
	writeFixedSizeString(e, l.server, 30, true)
	e.WriteByte(0x00)
	e.WriteInt8(int8(0))
	writeFixedSizeString(e, "", 254, false)
	e.Write(l.protocolVersion[:])
	writeFixedSizeString(e, l.library, 10, true)
	e.Write(l.libraryVersion[:])
	e.WriteInt8(0)  // convert short date
	e.WriteInt8(13) // format of flt32
	e.WriteInt8(17) // format of smalldate
	writeFixedSizeString(e, l.language, 30, true)
	e.WriteInt8(1)
	e.Pad(0x00, 2)
	e.WriteUint8(l.encrypted)
	e.Pad(0x00, 10)
	writeFixedSizeString(e, l.charset, 30, true)
	e.WriteInt8(1) // notify language change
	writeFixedSizeString(e, fmt.Sprintf("%d", l.packetSize), 6, true)
	// if no packet size was given, ask the server
	if l.packetSize == 0 {
		l.capabilities.setcapabilities(CapabilityReq, reqSrvpktsize)
	}
	e.Pad(0x00, 4) // magic
	err := e.Err()
	return err
}

func (l *login) Read(e *bin.Encoder) error {
	l.clientHost = readFixedSizeString(e, 30)
	l.user = readFixedSizeString(e, 30)
	l.password = readFixedSizeString(e, 30)
	l.pid = readFixedSizeString(e, 30)
	l.int2BE = e.Int8()
	e.Skip(15)
	l.app = readFixedSizeString(e, 30)
	l.server = readFixedSizeString(e, 30)
	e.Skip(256)
	e.Read(l.protocolVersion[:])
	l.library = readFixedSizeString(e, 10)
	e.Read(l.libraryVersion[:])
	e.Skip(3)
	l.language = readFixedSizeString(e, 30)
	e.Skip(14)
	l.charset = readFixedSizeString(e, 30)
	e.Skip(1)
	pktStr := readFixedSizeString(e, 6)
	l.packetSize, _ = strconv.Atoi(pktStr)
	e.Skip(4)
	l.capabilities.Read(e)
	err := e.Err()
	return err
}

//
// loginAck
//

// login status
const (
	LoginSuccessToken = 0x05
	LoginFailedToken  = 0x06
)

// LoginAck is the login ack packet
type loginAck struct {
	msg
	ack            int8
	tdsVersion     [4]byte
	serverFieldLen int8
	server         string
	serverVersion  [4]byte
}

// Write serializes a TdsLoginAck struct
func (l loginAck) Write(e *bin.Encoder) error {
	e.WriteByte(LoginSuccessToken)
	e.Write(l.tdsVersion[:])
	e.WriteStringWithLen(8, l.server)
	e.Write(l.serverVersion[:])
	err := e.Err()
	return err
}

// Read reads the LoginAck from the connection
func (l *loginAck) Read(e *bin.Encoder) error {
	l.ack = e.Int8()
	e.Read(l.tdsVersion[:])
	l.server, _ = e.ReadString(8)
	e.Read(l.serverVersion[:])
	err := e.Err()
	return err
}

//
// logout
//

type logout struct {
	msg
	option byte
}

func (l logout) Write(e *bin.Encoder) error {
	e.WriteByte(l.option)
	err := e.Err()
	return err
}

//
// dbRPC
//

type dbRPC struct {
	msg
	Name      string
	Flags     uint16
	HasParams bool
}

func (r *dbRPC) Read(e *bin.Encoder) error {
	r.Name, _ = e.ReadString(8)
	r.Flags = e.Uint16()
	// check for parameters
	r.HasParams = r.Flags&0x02 != 0
	err := e.Err()
	return err
}

//
// sqlMessage
//

// SybError is the struct containing sybase error information
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

// implement the error interface
func (e SybError) Error() string {

	if e.Procedure != "" {
		return fmt.Sprintf("Msg: %d, Level: %d, State: %d\nServer: %s, Procedure: %s, Line: %d:\n%s",
			e.MsgNumber, e.Severity, e.State, e.Server, e.Procedure, e.LineNumber, e.Message)
	}
	return fmt.Sprintf("Msg: %d, Level: %d, State: %d\nServer: %s, Line: %d:\n%s",
		e.MsgNumber, e.Severity, e.State, e.Server, e.LineNumber, e.Message)
}

// Max message size
const maxEedSize = 1024

// sqlMessage message structure.
type sqlMessage struct {
	msg
	SybError
}

func (m sqlMessage) Write(e *bin.Encoder) error {
	e.WriteInt32(m.MsgNumber)
	e.WriteInt8(m.State)
	e.WriteInt8(m.Severity)
	e.WriteStringWithLen(8, m.SQLState)
	e.WriteUint8(m.HasEed)
	e.WriteUint16(m.TranState)
	e.WriteStringWithLen(16, m.Message)
	e.WriteStringWithLen(8, m.Server)
	e.WriteStringWithLen(8, m.Procedure)
	e.WriteInt16(m.LineNumber)
	err := e.Err()
	return err
}

func (m *sqlMessage) Read(e *bin.Encoder) error {
	m.MsgNumber = e.Int32()
	m.State = e.Int8()
	m.Severity = e.Int8()
	m.SQLState, _ = e.ReadString(8)
	m.HasEed = e.Uint8()
	m.TranState = e.Uint16()
	m.Message, _ = e.ReadString(16)
	m.Server, _ = e.ReadString(8)
	m.Procedure, _ = e.ReadString(8)
	m.LineNumber = e.Int16()
	err := e.Err()
	return err
}

//
// option
//

// option cmd tokens
const (
	optionSet        = 0x01
	optionSetDefault = 0x02
	optionList       = 0x03
	optionInfo       = 0x04
)

// different setable options
const (
	_ = iota
	optionDateFirst
	optionTextSize
	optionStatTime
	optionStatIo
	optionRowCount
	optionNatLang
	optionDateFmt
	optionIsolationLevel
	optionAuthOn
	optionCharset
	_
	_
	optionShowplan
	optionNoexec
	optionArithignoreOn
	_
	optionArithAbortOn
	optionParseOnly
	_
	optionGetData
	optionNoCount
	_
	optionForcePlan
	optionFormatOnly
	optionChainXacts
	optionCurCloseOnXact
	optionFipsFlag
	optionRestrees
	optionIdentityOn
	optionCurRead
	optionCurWrite
	optionIdentityOff
	optionAuthOff
	optionAnsiNull
	optionQuotedIdent
	optionArithignoreOff
	optionArithAbortOff
	optionTruncAbort
)

const (
	_ = iota
	dateFirstMonday
	dateFirstTuesday
	dateFirstWednesday
	dateFirstThursday
	dateFirstFriday
	dateFirstSaturday
	dateFirstSunday
)

const (
	_ = iota
	dateFmtMDY
	dateFmtDMY
	dateFmtYMD
	dateFmtYDM
	dateFmtMYD
	dateFmtDYM
)

const (
	isolationReadUncommited = iota
	isolationReadCommited
	isolationRepeatableRead
	isolationSerializable
)

const isolationNotImplemented = -1

// map the options to their tds tokens, and give their different values when possible
var optionTypes = []reflect.Type{
	reflect.TypeOf(true), // placeholder
	reflect.TypeOf(int8(1)),
	reflect.TypeOf(int32(1)),
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(int32(1)),
	reflect.TypeOf(""),
	reflect.TypeOf(int8(1)),
	reflect.TypeOf(int8(1)),
	reflect.TypeOf(""),
	reflect.TypeOf(""),
	reflect.TypeOf(true), // placeholder
	reflect.TypeOf(true), // placeholder
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true), // placeholder
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true), // placeholder
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true), // placeholder
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(""),
	reflect.TypeOf(""), // unknown
	reflect.TypeOf(""), // unknown
	reflect.TypeOf(""),
	reflect.TypeOf(""),
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true),
	reflect.TypeOf(true),
}

// optionCmd is sent by the client to set options such as transaction chaining
type optionCmd struct {
	msg
	command int // 8 bits
	option  int // 8 bits
	value   interface{}
}

// Read unserializes a OptionCmd struct
func (c *optionCmd) Read(e *bin.Encoder) error {
	c.command = int(e.Int8())
	c.option = int(e.Int8())
	if c.option > len(optionTypes) {
		return fmt.Errorf("tds: unknown option (%#x)", c.option)
	}

	switch optionTypes[c.option] {
	case reflect.TypeOf(true):
		_ = e.Int8()
		if e.Int8() == 1 {
			c.value = true
		} else {
			c.value = false
		}
	case reflect.TypeOf(""):
		c.value, _ = e.ReadString(8)
	case reflect.TypeOf(int32(1)):
		_ = e.Int8()
		c.value = e.Int32()
	case reflect.TypeOf(int8(1)):
		_ = e.Int8()
		c.value = e.Int8()
	}
	err := e.Err()
	return err
}

// Read unserializes a OptionCmd struct
func (c optionCmd) Write(e *bin.Encoder) error {
	if c.option > len(optionTypes) {
		return fmt.Errorf("tds: unknown option: %#x", c.option)
	}
	e.WriteInt8(int8(c.command))
	e.WriteInt8(int8(c.option))

	switch optionTypes[c.option] {
	case reflect.TypeOf(true):
		e.WriteInt8(1)
		value, _ := c.value.(bool)
		if value {
			e.WriteInt8(1)
		} else {
			e.WriteInt8(0)
		}
	case reflect.TypeOf(""):
		value, _ := c.value.(string)
		e.WriteStringWithLen(8, value)
	case reflect.TypeOf(int32(1)):
		e.WriteInt8(4)
		value, _ := c.value.(int)
		e.WriteInt32(int32(value))
	case reflect.TypeOf(int8(1)):
		e.WriteInt8(1)
		value, _ := c.value.(int)
		e.WriteInt8(int8(value))
	}
	err := e.Err()
	return err
}

//
// returnStatus
//

type returnStatus struct {
	msg
	status int32
}

// Write serializes a done struct
func (r returnStatus) Write(e *bin.Encoder) error {
	e.WriteInt32(r.status)
	err := e.Err()
	return err
}

// Read serializes a done struct
func (r *returnStatus) Read(e *bin.Encoder) error {
	r.status = e.Int32()
	err := e.Err()
	return err
}

//
// sybMsg
//

type sybMsg struct {
	msg
	field1 int8
	field2 int16
}

// Write serializes a sybMsg struct
func (m sybMsg) Write(e *bin.Encoder) error {
	e.WriteInt8(m.field1)
	e.WriteInt16(m.field2)
	err := e.Err()
	return err
}
