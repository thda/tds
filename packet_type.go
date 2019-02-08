package tds

//go:generate stringer -type=packetType

// packetType is a tds packetType. Represented by a single byte
// It precedes every PDU and every message
type packetType byte

// netlib Tokens
const (
	nonePacket   packetType = 0x00 // no Token
	queryPacket  packetType = 0x01
	loginPacket  packetType = 0x02
	rpcPacket    packetType = 0x03
	replyPacket  packetType = 0x04
	cancelPacket packetType = 0x06
	bulkPacket   packetType = 0x07
	normalPacket packetType = 0x0f // 15
)
