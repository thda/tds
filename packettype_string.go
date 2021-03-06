// Code generated by "stringer -type=packetType"; DO NOT EDIT.

package tds

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[nonePacket-0]
	_ = x[queryPacket-1]
	_ = x[loginPacket-2]
	_ = x[rpcPacket-3]
	_ = x[replyPacket-4]
	_ = x[cancelPacket-6]
	_ = x[bulkPacket-7]
	_ = x[normalPacket-15]
}

const (
	_packetType_name_0 = "nonePacketqueryPacketloginPacketrpcPacketreplyPacket"
	_packetType_name_1 = "cancelPacketbulkPacket"
	_packetType_name_2 = "normalPacket"
)

var (
	_packetType_index_0 = [...]uint8{0, 10, 21, 32, 41, 52}
	_packetType_index_1 = [...]uint8{0, 12, 22}
)

func (i packetType) String() string {
	switch {
	case 0 <= i && i <= 4:
		return _packetType_name_0[_packetType_index_0[i]:_packetType_index_0[i+1]]
	case 6 <= i && i <= 7:
		i -= 6
		return _packetType_name_1[_packetType_index_1[i]:_packetType_index_1[i+1]]
	case i == 15:
		return _packetType_name_2
	default:
		return "packetType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
}
