package binary

// Encoder provides encoding facilities to write binary data //
// with the proper endianness.
// It can also read/write strings prefixed with their lengths,
// and handle charset conversions.

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"

	"golang.org/x/text/encoding"
)

const maxTextSize = 100000

// Encoder without charset conversion
type Encoder struct {
	rw          io.ReadWriter    // readWriter
	r           io.Reader        // reader. Used to switch to a limited reader
	sbuf        [8]byte          // scratch buffer for endianness conversion
	endianness  binary.ByteOrder // binary encoding
	charset     encoding.Encoding
	charEncoder *encoding.Encoder
	charDecoder *encoding.Decoder
	err         error
}

// NewEncoder returns an Encoder without charset conversion
func NewEncoder(rw io.ReadWriter, endianness binary.ByteOrder) Encoder {
	encoder := Encoder{rw: rw, r: rw, endianness: endianness}
	return encoder
}

// NewEncoderCharset returns an Encoder which handles charset conversion
func NewEncoderCharset(rw io.ReadWriter, endianness binary.ByteOrder,
	charset encoding.Encoding) (e Encoder, err error) {
	encoder := Encoder{rw: rw, r: rw, endianness: endianness, charset: charset}
	err = encoder.SetCharset(charset)
	return encoder, err
}

// SetEndianness changes the endianness
func (erw *Encoder) SetEndianness(endianness binary.ByteOrder) {
	erw.endianness = endianness
}

// Endianness returns the byte order of the Encoder
func (erw Encoder) Endianness() binary.ByteOrder {
	return erw.endianness
}

// SetCharset changes the charset.
// Expects the html name of the charset.
func (erw *Encoder) SetCharset(c encoding.Encoding) error {
	erw.charset = c
	erw.charEncoder, erw.charDecoder = c.NewEncoder(), c.NewDecoder()
	return erw.err
}

// Write implements io.Writer
func (erw *Encoder) Write(b []byte) (cnt int, err error) {
	if erw.err != nil {
		return
	}
	cnt, erw.err = erw.rw.Write(b[:])
	return cnt, erw.err
}

// Read implements io.Reader
func (erw *Encoder) Read(data []byte) (cnt int, err error) {
	if erw.err != nil {
		return
	}
	cnt, erw.err = io.ReadFull(erw.r, data[:])
	return cnt, erw.err
}

// LimitRead limits the read to n bytes
func (erw *Encoder) LimitRead(n int64) {
	erw.r = io.LimitReader(erw.rw, n)
}

// UnlimitRead removes the read limitation
func (erw *Encoder) UnlimitRead() {
	erw.r = erw.rw
}

// Err reads the last error from the encoder
// and resets the error status
func (erw *Encoder) Err() (err error) {
	err = erw.err
	erw.err = nil
	return err
}

// shortcuts for ascii strings
var asciire = regexp.MustCompile("[[:^ascii:]]")

func (erw *Encoder) WriteAsciiStringWithLen(lenSize int, data string) {
	erw.WriteStringWithLen(lenSize, asciire.ReplaceAllLiteralString(data, "?"))
}

func (erw *Encoder) WriteAsciiString(data string) int {
	return erw.WriteString(asciire.ReplaceAllLiteralString(data, "?"))
}

// WriteString writes a string to the underlying writer
func (erw *Encoder) WriteString(data string) int {
	if erw.err != nil {
		return 0
	}

	buf := []byte(data)

	// check if encoder is needed
	if erw.charEncoder != nil {
		buf, erw.err = erw.charEncoder.Bytes(buf)
	}

	cnt, _ := erw.Write(buf)
	return cnt
}

// WriteStringWithLen writes a string along with its lenght.
// The first parameter gives the size of the length in bytes.
// When character set encoding is necessary, the length
// is computed after conversion and properly sent.
func (erw *Encoder) WriteStringWithLen(lenSize int, data string) {
	if erw.err != nil {
		return
	}

	// check if encoder is needed
	if erw.charEncoder != nil {
		data, erw.err = erw.charEncoder.String(data)
		if erw.err != nil {
			return
		}
	}

	stringLen := len([]byte(data))
	switch lenSize {
	case 8:
		erw.WriteUint8(uint8(stringLen))
	case 16:
		erw.WriteUint16(uint16(stringLen))
	case 32:
		erw.WriteUint32(uint32(stringLen))
	case 64:
		erw.WriteUint64(uint64(stringLen))
	}
	if erw.err != nil {
		return
	}
	if stringLen == 0 {
		return
	}
	erw.Write([]byte(data))
}

func (erw *Encoder) ReadString(lenSize int) (data string, size int) {
	if erw.err != nil {
		return
	}
	switch lenSize {
	case 8:
		size = int(erw.Uint8())
	case 16:
		size = int(erw.Uint16())
	case 32:
		size = int(erw.Uint32())
	}
	if erw.err != nil {
		return
	}
	if size == 0 {
		return
	}

	return erw.ReadStringWithLen(size), size
}

func (erw *Encoder) ReadStringWithLen(len int) (data string) {
	if erw.err != nil {
		return
	}
	if len < 0 || len > maxTextSize {
		erw.err = fmt.Errorf("encoder: invalid string length: %d", len)
		return
	}
	if len == 0 {
		return ""
	}
	buf := make([]byte, len)
	erw.Read(buf[:])
	if erw.err != nil {
		return
	}

	// check if decoding is needed
	if erw.charDecoder != nil {
		out, err := erw.charDecoder.Bytes(buf)
		erw.err = err
		return string(out)
	}

	return string(buf)
}

func (erw *Encoder) WriteByte(b byte) {
	if erw.err != nil {
		return
	}
	erw.sbuf[0] = b
	_, erw.err = erw.rw.Write(erw.sbuf[:1])
}

func (erw *Encoder) Pad(b byte, cnt int) {
	if erw.err != nil {
		return
	}
	for i := 0; i < cnt; i++ {
		erw.WriteByte(b)
	}
}

func (erw *Encoder) Skip(cnt int64) {
	if erw.err != nil {
		return
	}
	_, erw.err = io.CopyN(ioutil.Discard, erw.rw, cnt)
}

func (erw *Encoder) WriteInt8(i int8) {
	if erw.err != nil {
		return
	}
	erw.sbuf[0] = byte(i)
	erw.Write(erw.sbuf[:1])
}

func (erw *Encoder) WriteUint8(i uint8) {
	if erw.err != nil {
		return
	}
	erw.sbuf[0] = byte(i)
	erw.Write(erw.sbuf[:1])
}

func (erw *Encoder) WriteInt16(i int16) {
	if erw.err != nil {
		return
	}
	erw.endianness.PutUint16(erw.sbuf[:2], uint16(i))
	erw.Write(erw.sbuf[:2])
}

func (erw *Encoder) WriteUint16(i uint16) {
	if erw.err != nil {
		return
	}
	erw.endianness.PutUint16(erw.sbuf[:2], i)
	erw.Write(erw.sbuf[:2])
}

func (erw *Encoder) WriteInt32(i int32) {
	if erw.err != nil {
		return
	}
	erw.endianness.PutUint32(erw.sbuf[:4], uint32(i))
	erw.Write(erw.sbuf[:4])
}

func (erw *Encoder) WriteUint32(i uint32) {
	if erw.err != nil {
		return
	}
	erw.endianness.PutUint32(erw.sbuf[:4], i)
	erw.Write(erw.sbuf[:4])
}

func (erw *Encoder) WriteInt64(i int64) {
	if erw.err != nil {
		return
	}
	erw.endianness.PutUint64(erw.sbuf[:8], uint64(i))
	erw.Write(erw.sbuf[:8])
}

func (erw *Encoder) WriteUint64(i uint64) {
	if erw.err != nil {
		return
	}
	erw.endianness.PutUint64(erw.sbuf[:8], i)
	erw.Write(erw.sbuf[:8])
}

func (erw *Encoder) ReadByte() (b byte) {
	if erw.err != nil {
		return
	}
	erw.Read(erw.sbuf[:1])
	return erw.sbuf[0]
}

func (erw *Encoder) Uint8() (i uint8) {
	if erw.err != nil {
		return
	}
	erw.Read(erw.sbuf[:1])
	return uint8(erw.sbuf[0])
}

func (erw *Encoder) Int8() (i int8) {
	if erw.err != nil {
		return
	}
	return int8(erw.Uint8())
}

func (erw *Encoder) Uint16() (i uint16) {
	if erw.err != nil {
		return
	}
	erw.Read(erw.sbuf[:2])
	return erw.endianness.Uint16(erw.sbuf[:2])
}

func (erw *Encoder) Int16() (i int16) {
	if erw.err != nil {
		return
	}
	return int16(erw.Uint16())
}

func (erw *Encoder) Uint32() (i uint32) {
	if erw.err != nil {
		return
	}
	erw.Read(erw.sbuf[:4])
	return erw.endianness.Uint32(erw.sbuf[:4])
}

func (erw *Encoder) Int32() (i int32) {
	if erw.err != nil {
		return
	}
	return int32(erw.Uint32())
}

func (erw *Encoder) Uint64() (i uint64) {
	if erw.err != nil {
		return
	}
	erw.Read(erw.sbuf[:8])
	return erw.endianness.Uint64(erw.sbuf[:8])
}

func (erw *Encoder) Int64() (i int64) {
	if erw.err != nil {
		return
	}
	return int64(erw.Uint64())
}
