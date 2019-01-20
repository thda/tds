package netlib

// All packet and messages encapsulation goes here.
// No protocol logic except bytes shuffling.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	bin "github.com/thda/tds/binary"
)

// headerSize is the size of the tds header
const headerSize = 8

const (
	_ = iota
	eom
	cancelAck
	cancel
)

// header is the header for netlib packets which enclose all messages
type header struct {
	token      PacketType
	status     uint8
	packetSize uint16
	spid       uint16
	packetNo   uint8
	pad        uint8
}

// Read deserializes a PacketHeader struct
func (h *header) read(e *bin.Encoder) error {
	h.token = PacketType(e.ReadByte())
	h.status = e.Uint8()
	h.packetSize = e.Uint16()
	h.spid = e.Uint16()
	h.packetNo = e.Uint8()
	h.pad = e.Uint8()
	err := e.Err()
	return err
}

// Write serializes a PacketHeader struct
func (h header) write(e *bin.Encoder) error {
	e.WriteByte(byte(h.token))
	e.WriteByte(h.status)
	e.WriteUint16(h.packetSize)
	e.WriteUint16(h.spid)
	e.WriteUint8(h.packetNo)
	e.WriteUint8(h.pad)
	err := e.Err()
	return err
}

// Messager is the interface which describes
// the messages sent by the netlib buffer.
//
// A message should provide serialization functions
// as well as a token and clues on its size.
type Messager interface {
	Token() byte
	Size() uint8
	SizeLen() uint8
	LimitRead() bool
	Write(*bin.Encoder) error
	Read(*bin.Encoder) error
}

const maxMsgBufSize = 25000

// DefaultCancelTimeout is the number of seconds to wait for the cancel to be sent
const DefaultCancelTimeout = 10

// Buffer reads and writes netlib packets with proper header and size
type Buffer struct {
	rw io.ReadWriter
	h  header       // packet header
	pb bytes.Buffer // packet buffer
	d  [50]byte     // discard buffer
	mb bytes.Buffer // message buffer, used to easily compute message length
	me bin.Encoder  // message encoder. This one is buffered
	he bin.Encoder  // header encoder. Reads from the network, writes to the write buffer
	// packet encoder. Reads/Writes goes to this structure's read/write function to split into TDS packets.
	pe         bin.Encoder
	debug      bool
	PacketSize int

	// Timeouts/context variables
	cancelCh      chan error // chanel to inform on cancel completion
	inCancel      int32      // set to 1 if a cancel query is pending
	WriteTimeout  int
	ReadTimeout   int
	CancelTimeout int // number of seconds before cancel is timed out and connection is marked dead

	DefaultMessageMap map[byte]Messager
}

// NewBuffer inits a buffer struct with the different buffers for packet, message and header
func NewBuffer(packetSize int, rw io.ReadWriter) *Buffer {
	b := new(Buffer)
	b.PacketSize = packetSize
	b.rw = rw
	b.me = bin.NewEncoder(&b.mb, binary.LittleEndian)
	b.pe = bin.NewEncoder(b, binary.LittleEndian)
	b.he = bin.NewEncoder(&struct {
		io.Reader
		io.Writer
	}{b.rw, &b.pb}, binary.BigEndian)
	b.cancelCh = make(chan error, 1)
	b.CancelTimeout = DefaultCancelTimeout
	return b
}

// SetEndianness changes the endianness for the packet encoder and the packet buffer
func (b *Buffer) SetEndianness(endianness binary.ByteOrder) {
	b.pe.SetEndianness(endianness)
	b.me.SetEndianness(endianness)
}

// SetCharset changes the charset for the packet encoder and the packet buffer
func (b *Buffer) SetCharset(c string) error {
	e, err := getEncoding(c)
	if err != nil {
		return fmt.Errorf("netlib: could not find encoder for %s", c)
	}
	b.pe.SetCharset(e)
	b.me.SetCharset(e)
	return nil
}

// initPkt sets the packet type and send the header.
// Usually called whenever the packet type changes and after a message send,
// when other messages are expected
func (b *Buffer) initPkt(t PacketType) {
	b.pb.Reset()
	b.h.token, b.h.status = t, 0
	b.h.write(&b.he)
}

// readPkt reads a tds packet and fills the header information
func (b *Buffer) readPkt(ignoreCan bool) (err error) {
	b.pb.Reset()

	// Actually read packet
	if err = b.h.read(&b.he); err != nil {
		return err
	}
	if _, err = io.CopyN(&b.pb, b.rw, int64(b.h.packetSize)-headerSize); err != nil {
		return err
	}

	// check for cancel signal
	if !ignoreCan && b.cancelling() {
		err = b.processCancel()
	}

	return err
}

// sendPkt sends a packet to the underlying writer
// It writes the header, the payload and flushes if needed
func (b *Buffer) sendPkt(status uint8) (err error) {
	b.pb.Bytes()[1] = status

	// discard packets until the last one when cancelling.
	// When a cancel is spotter, we must first send
	// a packet with cancel and eom bit set,
	// then ignore all the next packets until the caller
	// indicates the end of the conversation by calling
	// sendPkt giving a status parameter with eom bit set.
	if b.cancelling() {
		if b.h.status&cancel == cancel &&
			status&eom != eom {
			return nil
		}
		b.h.status |= cancel
		b.pb.Bytes()[1] = eom | cancel
	}

	// set packet length and status
	binary.BigEndian.PutUint16(b.pb.Bytes()[2:], uint16(b.pb.Len()))

	// single call to write, needed for concurrent writes.
	_, err = b.pb.WriteTo(b.rw)

	// not the last packet, write header for next
	if status&eom == 0 {
		b.initPkt(b.h.token)
	} else if b.cancelling() {
		// last packet of a canceled request, process cancel ack
		b.h.status = 0
		return b.processCancel()
	}

	return err
}

// Read reads from the reader and fills the scratch buffer buf.
// Will also return the number of bytes read.
// Eventually reads the next packet if needed.
// Implements the io.Reader interface
func (b *Buffer) Read(buf []byte) (n int, err error) {
	n, err = io.ReadFull(&b.pb, buf)

	// could not read all now, proceed next packet
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		if err = b.readPkt(false); err != nil {
			return 0, err
		}
	} else if n == 0 && b.h.status == 1 {
		// all data read
		return 0, io.EOF
	}

	return n, err
}

// Write writes the buffer's data to the underlying writer.
// We flush whenever we fill the write buffer using sendPacket.
// Implements the io.Writer interface
func (b *Buffer) Write(p []byte) (n int, err error) {
	var copied, remaining int
	for {
		remaining = int(b.PacketSize) - b.pb.Len()

		// check if the write would fill the current packet.
		if len(p) >= remaining {
			n, err = b.pb.Write(p[:remaining])
		} else {
			n, err = b.pb.Write(p[:])
		}

		if err != nil {
			return 0, err
		}

		p = p[n:]
		copied += n
		if len(p) == 0 {
			return copied, nil
		}

		// not all data was copied in this batch, flush packet
		if err = b.sendPkt(0); err != nil {
			return copied, err
		}
	}
}

// Skip skips a given amount of bytes
func (b *Buffer) skip(cnt int) (err error) {
	if cnt == 0 {
		return nil
	}
	// optimize for a small skip. Usually done tokens
	if cnt < len(b.d) {
		_, err = io.ReadFull(b, b.d[:cnt])
		return err
	}

	for skipped := 0; skipped < cnt; skipped += len(b.d) {
		if cnt-skipped < len(b.d) {
			_, err = io.ReadFull(b, b.d[:cnt%len(b.d)])
		} else {
			_, err = io.ReadFull(b, b.d[:])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Peek will read one byte without affecting the offset
func (b *Buffer) Peek() (out byte, err error) {
	out = b.pe.ReadByte()
	err = b.pe.Err()
	b.pb.UnreadByte()
	return out, err
}

// writeMsg writes the message tok, computes the message size
// and writes it to the underlying writer.
// This is used when the tds needs a length right after the token
// for non-fixed length messages
func (b *Buffer) writeMsg(msg Messager) (err error) {
	b.mb.Reset()

	if err = msg.Write(&b.me); err != nil {
		return err
	}

	if msg.Token() != byte(None) {
		b.pe.WriteByte(msg.Token())
	}

	// if it's a token with a known size, write it
	switch msg.SizeLen() {
	case 8:
		b.pe.WriteInt8(int8(b.mb.Len()))
	case 16:
		b.pe.WriteInt16(int16(b.mb.Len()))
	case 32:
		b.pe.WriteInt32(int32(b.mb.Len()))
	}

	if err = b.pe.Err(); err != nil {
		return err
	}

	// Write to packet buffer
	_, err = b.mb.WriteTo(b)

	// reset buffer and check for its size
	if b.mb.Cap() > maxMsgBufSize {
		b.mb = *new(bytes.Buffer)
	}

	return err
}

// readMsg reads a message from the underlying connection.
func (b *Buffer) readMsg(msg Messager) (err error) {
	var size int
	switch msg.SizeLen() {
	case 8:
		size = int(b.pe.Uint8())
	case 16:
		size = int(b.pe.Uint16())
	case 32:
		size = int(b.pe.Uint32())
	}
	if err = b.pe.Err(); err != nil {
		return err
	}

	// For some messages, we have no way to know the end of a field/a serie of fields
	// before reaching the last byte, as given by the packet size field.
	// For those, we set the Encoder's reader to a limitedReader which
	// will signal the end of processing by io.EOF.
	// Reverted afterwards.
	if msg.LimitRead() {
		b.pe.LimitRead(int64(size))
		defer func(e *bin.Encoder) {
			e.UnlimitRead()
		}(&b.pe)
	}

	return msg.Read(&b.pe)
}

// skipMsg skips a message according to its length.
func (b *Buffer) skipMsg(msg Messager) (err error) {
	var size int
	// check for existence

	if msg.Size() != 0 {
		size = int(msg.Size())
	} else {
		switch msg.SizeLen() {
		default:
			return fmt.Errorf("netlib: unknown token size for %s message", msg)
		case 8:
			size = int(b.pe.Uint8())
		case 16:
			size = int(b.pe.Uint16())
		case 32:
			size = int(b.pe.Uint32())
		}
	}

	err = b.skip(size)
	return err
}

// Send sends a list of messages given as parameters
func (b *Buffer) Send(ctx context.Context, pt PacketType, msgs ...Messager) (err error) {
	// init packet header
	b.initPkt(pt)

	// create a context with a Timeout of WriteTimeout if no particular context given
	if ctx == nil && b.WriteTimeout > 0 {
		var cancelFunc func()
		ctx, cancelFunc = context.WithTimeout(context.Background(), time.Duration(b.WriteTimeout)*time.Second)
		defer cancelFunc()
	}

	// start Timeout watcher
	if ctx != nil {
		if cancel := b.watchCancel(ctx, false); cancel != nil {
			defer cancel()
		}
	}

	// send messages
	for _, msg := range msgs {
		if err = b.writeMsg(msg); err != nil {
			return err
		}
	}

	// flush
	return b.sendPkt(1)
}

// Receive reads messages given in a map, and run a given message handler after each message read.
//
// When the handler returns true, it will return instantly and quit processing messages.
// When there is not matching message in the map, the getMsg function will be called
// to get the message's length and skip it.
//
// Returns the token of the last parsed message, and eventually an error
func (b *Buffer) Receive(ctx context.Context, msgs map[byte]Messager,
	msgHandler func(byte, bool) (bool, error), doBreak bool,
	getMsg func(byte) Messager) (byte, error) {

	// create a context with a Timeout of ReadTimeout if no particular context given
	if ctx == nil && b.ReadTimeout > 0 {
		var cancelFunc func()
		ctx, cancelFunc = context.WithTimeout(context.Background(), time.Duration(b.ReadTimeout)*time.Second)
		defer cancelFunc()
	}

	// start Timeout watcher
	if ctx != nil {
		if cancel := b.watchCancel(ctx, true); cancel != nil {
			defer cancel()
		}
	}

	var t byte
	var err error
	for {
		t = b.pe.ReadByte()
		if err = b.pe.Err(); err != nil {
			// we should not be at EOF here
			if err == io.EOF {
				return 0, fmt.Errorf("netlib: unexpected EOF while reading message")
			}
			return 0, err
		}

		// expecting reply here
		if b.h.token != Normal && b.h.token != Reply {
			return t, fmt.Errorf("netlib: expected reply or normal token, got %s", b.h.token)
		}

		// check if the message is in the ones to return
		// and attempt to skip if not found
		msg, ok := b.DefaultMessageMap[t]

		// look in provided message map
		if !ok {
			msg, ok = msgs[t]
		}

		if !ok {
			if err = b.skipMsg(getMsg(t)); err != nil {
				return t, err
			}
		} else {
			// read the message and call the callback
			err = b.readMsg(msg)
			if err != nil {
				return t, err
			}
			doBreak, err := msgHandler(t, doBreak)
			if doBreak || err != nil {
				return t, err
			}
		}
	}
}

// watchCancel will start a cancelation goroutine
// if the context can be terminated.
// Returns a function to end the goroutine
func (b *Buffer) watchCancel(ctx context.Context, reading bool) func() {
	if done := ctx.Done(); done != nil {
		finished := make(chan struct{})
		go func() {
			select {
			case <-done:
				_ = b.cancel(ctx.Err(), reading)
				finished <- struct{}{}
			case <-finished:
			}
		}()
		return func() {
			select {
			case <-finished:
			case finished <- struct{}{}:
			}
		}
	}
	return nil
}

// cancel simply sends a cancel message to the cancel channel.
func (b *Buffer) cancel(cancelErr error, reading bool) (err error) {
	if swapped := atomic.CompareAndSwapInt32(&b.inCancel, 0, 1); !swapped {
		// cancel already in progress
		return nil
	}

	// send to the cancel channel when the cancel is sent or an error is faced.
	defer func() {
		b.cancelCh <- cancelErr
	}()

	// set deadline on the underlying conn to be sure to process on time
	if conn, ok := b.rw.(net.Conn); ok {
		defer conn.SetDeadline(time.Time{})
		err = conn.SetDeadline(time.Now().Add(time.Duration(b.CancelTimeout) * time.Second))
		if err != nil {
			return err
		}
	}

	// we are currently reading, so we need to send a cancel packet
	// to avoid draining cancel channel
	if reading {
		canBuf := NewBuffer(int(b.h.packetSize), b.rw)
		canBuf.initPkt(Cancel)
		err = canBuf.sendPkt(1)
	}
	return err
}

// cancelling checks if a cancel was requested.
func (b *Buffer) cancelling() bool {
	return atomic.LoadInt32(&b.inCancel) == 1
}

// processCancel reads packets until finding the cancel ack.
func (b *Buffer) processCancel() (err error) {
	var cancelErr error
	defer atomic.StoreInt32(&b.inCancel, 0)

	// this will effectively block until cancel packet is sent
	select {
	case cancelErr = <-b.cancelCh:
	}

	// read until last packet
	for {
		// last packet read
		if b.h.status&eom != 0 {
			break
		}
		b.readPkt(true)
	}

	// the server has 2 ways to send cancel ack:
	//	- a normal packet with headerCancelAck status bit set
	//  - a reply packet containing a done message with doneCancel bit set
	switch b.h.token {
	default:
		err = fmt.Errorf("netlib: unexpected token type %s while looking for cancel token", b.h.token)
	case Normal:
		err = cancelErr
		if b.h.status&cancelAck == 0 {
			err = errors.New("netlib: Timeout reached, yet the cancel was not acknowledged")
		}
	case Reply:
		if err = b.skip(b.pb.Len() - 9); err == nil {
			err = cancelErr
			// find done token with cancel ack bit set
			if !(b.pe.ReadByte() == 0xFD && int(b.pe.Uint16())&0x0020 != 0) {
				err = errors.New("netlib: Timeout reached, yet the cancel was not acknowledged")
			}
		}
	}
	return err
}
