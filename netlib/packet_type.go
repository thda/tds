package netlib

//go:generate stringer -type=PacketType

// PacketType is a tds PacketType. Represented by a single byte
// It precedes every PDU and every message
type PacketType byte

// netlib Tokens
const (
	None   PacketType = 0x00 // no Token
	Query  PacketType = 0x01
	Login  PacketType = 0x02
	RPC    PacketType = 0x03
	Reply  PacketType = 0x04
	Cancel PacketType = 0x06
	Bulk   PacketType = 0x07
	Normal PacketType = 0x0f // 15
)
