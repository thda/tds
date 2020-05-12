package tds

import (
	"database/sql/driver"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/thda/tds/binary"
	"golang.org/x/text/encoding/unicode"

	"errors"
)

//go:generate stringer -type=dataType
type dataType byte

// data types
const (
	uInt4Type    dataType = 0x1d //  29
	imageType    dataType = 0x22 //  34
	textType     dataType = 0x23 //  35
	extendedType dataType = 0x24 //  36
	// Timestamp      dataType = 0x25 //  37
	varbinaryType dataType = 0x25 //  37
	intNType      dataType = 0x26 //  38
	// Sysname        dataType = 0x27 //  39
	varcharType dataType = 0x27 //  39
	// Nvarchar       dataType = 0x27 //  39
	// Longsysname    dataType = 0x27 //  39
	binaryType dataType = 0x2d //  45
	charType   dataType = 0x2f //  47
	// Nchar          dataType = 0x2f //  47
	tinyintType        dataType = 0x30 //  48
	dateType           dataType = 0x31 //  49
	bitType            dataType = 0x32 //  50
	timeType           dataType = 0x33 //  51
	smallintType       dataType = 0x34 //  52
	decimalType        dataType = 0x37 //  55
	intType            dataType = 0x38 //  56
	smalldatetimeType  dataType = 0x3a //  58
	realType           dataType = 0x3b //  59
	moneyType          dataType = 0x3c //  60
	datetimeType       dataType = 0x3d //  61
	floatType          dataType = 0x3e //  62
	numericType        dataType = 0x3f //  63
	uTinyintType       dataType = 0x40 //  64
	uSmallintType      dataType = 0x41 //  65
	uIntType           dataType = 0x42 //  66
	uBigintType        dataType = 0x43 //  67
	uIntNType          dataType = 0x44 //  68
	bigDateTimeNType   dataType = 0x50 //  80
	decimalNType       dataType = 0x6a // 106
	numericNType       dataType = 0x6c // 108
	floatNType         dataType = 0x6d // 109
	moneyNType         dataType = 0x6e // 110
	datetimeNType      dataType = 0x6f // 111
	smallmoneyType     dataType = 0x7a // 122
	dateNType          dataType = 0x7b // 123
	unicharType        dataType = 0x87 // 135
	timeNType          dataType = 0x93 // 147
	univarcharType     dataType = 0x9b // 155
	textLocatorType    dataType = 0xa9 // 169
	imageLocatorType   dataType = 0xaa // 170
	unitextLocatorType dataType = 0xab // 171
	unitextType        dataType = 0xae // 174
	longCharType       dataType = 0xaf // 175
	bigdatetimeNType   dataType = 0xbb // 187
	bigtimeNType       dataType = 0xbc // 188
	bigdatetimeType    dataType = 0xbd // 189
	bigtimeType        dataType = 0xbe // 190
	bigintType         dataType = 0xbf // 191
	longBinaryType     dataType = 0xe1 // 225
)

// Map user type with its concrete type.
//
// each type info has a user type which maps to the concrete type.
// For example, some type are encoded on the wire with the Intn data type,
// but have the user type field set to TinyInt, Smallint.
// Another example is the timestamp, which in actually a varbinary(8).
// This table establishes the mapping between user type and the underlying data type.
var concreteTypes = []dataType{
	1:  charType,
	2:  varcharType,
	3:  binaryType,
	4:  varbinaryType,
	5:  tinyintType,
	6:  smallintType,
	7:  intType,
	8:  floatType,
	10: numericType,
	11: moneyType,
	12: datetimeType,
	13: intNType,
	14: floatNType,
	15: datetimeNType,
	16: bitType,
	17: moneyNType,
	18: varcharType,
	19: textType,
	20: imageType,
	21: smallmoneyType,
	22: smalldatetimeType,
	23: realType,
	24: charType,
	25: varcharType,
	26: decimalType,
	27: decimalNType,
	28: numericNType,
	29: uIntType,
	33: bigintType,
	34: unicharType,
	35: univarcharType,
	36: unitextType,
	37: dateType,
	38: timeType,
	39: dateNType,
	40: timeNType,
	42: intNType,
	43: bigintType,
	44: uIntNType,
	45: uIntNType,
	46: uBigintType,
	47: uIntNType,
	48: bigdatetimeType,
	49: bigtimeType,
	50: bigdatetimeNType,
	51: bigtimeNType,
	53: decimalType,
	54: decimalType,
	55: decimalType,
	80: varbinaryType,
	84: bigintType,
}

// typeAttribute is the struct containing all the attributes of a sybase type
type typeAttribute struct {
	options      uint8
	name         string
	numBytes     int8 // length in case of a fixed-width type
	scanType     reflect.Type
	encodingType dataType // datatype used for encoding
	writer       func(*binary.Encoder, interface{}, colType) error
	reader       func(*binary.Encoder, colType) (interface{}, error)
	converter    func(*colFmt) driver.ValueConverter
}

// Type options
const (
	noOption        = 0
	hasLength uint8 = 1 << iota
	hasPrec
	hasScale
	isNullable
	isLong
	isLob
	isConcrete
)

// init custom converters, which check for overflows
var tinyIntConv = &intConverter{max: math.MaxUint8, min: 0}
var smallIntConv = &intConverter{max: math.MaxInt16, min: math.MinInt16}
var nullIntConv = &intConverter{max: math.MaxInt32, min: math.MinInt32}
var intConv = &intConverter{max: math.MaxInt32, min: math.MinInt32}
var bigIntConv = &intConverter{max: math.MaxInt64, min: math.MinInt64}

var uIntConv = &intConverter{max: math.MaxUint32, min: 0}
var nullUTinyIntConv = &intConverter{max: math.MaxUint8, min: 0}
var nullUSmallIntConv = &intConverter{max: math.MaxUint16, min: 0}
var uSmallIntConv = &intConverter{max: math.MaxUint16, min: 0}
var uBigIntConv = &intConverter{max: math.MaxUint64, min: 0}

var boolConv = &boolConverter{}
var dateTimeConv = &dateConverter{hasDate: true, hasTime: true,
	min: time.Date(1753, time.Month(1), 1, 0, 0, 0, 0, time.Local),
	max: time.Date(9999, time.Month(12), 31, 23, 59, 59, 999999999, time.Local)}
var dateConv = &dateConverter{hasDate: true, hasTime: false,
	min: time.Date(0001, time.Month(1), 1, 0, 0, 0, 0, time.Local),
	max: time.Date(9999, time.Month(12), 31, 23, 59, 59, 999999999, time.Local)}
var timeConv = &dateConverter{hasDate: false, hasTime: true}
var smallDateTimeConv = &dateConverter{hasDate: true, hasTime: true,
	min: time.Date(1900, time.Month(1), 1, 0, 0, 0, 0, time.Local),
	max: time.Date(2079, time.Month(6), 6, 23, 59, 59, 999999999, time.Local)}
var bigDateTimeConv = &dateConverter{hasDate: true, hasTime: true,
	min: time.Date(0001, time.Month(1), 1, 0, 0, 0, 0, time.Local),
	max: time.Date(9999, time.Month(12), 31, 23, 59, 59, 999999999, time.Local)}

var float32Conv = &floatConverter{max: math.MaxFloat32}
var float64Conv = &floatConverter{max: math.MaxFloat64}

var charConv = &typeCheckConverter{expectedType: reflect.TypeOf("")}
var byteConv = &typeCheckConverter{expectedType: reflect.TypeOf([]byte{})}

var charFct = func(*colFmt) driver.ValueConverter { return charConv }
var byteFct = func(*colFmt) driver.ValueConverter { return byteConv }

// typeAttributes is a table which contains the attributes for each Token
var typeAttributes = map[dataType]typeAttribute{
	binaryType:        {isConcrete | hasLength, "binary", 0, reflect.TypeOf([]byte{}), binaryType, encodeBinary, decodeBinary, byteFct},
	bitType:           {isConcrete, "bit", 1, reflect.TypeOf(true), bitType, encodeBool, decodeBool, func(*colFmt) driver.ValueConverter { return boolConv }},
	charType:          {isConcrete | hasLength, "char", 0, reflect.TypeOf(""), charType, encodeChar, decodeChar, charFct},
	datetimeType:      {isConcrete, "datetime", 8, reflect.TypeOf(time.Time{}), datetimeNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return dateTimeConv }},
	dateType:          {isConcrete, "date", 4, reflect.TypeOf(time.Time{}), dateNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return dateConv }},
	timeType:          {isConcrete, "time", 4, reflect.TypeOf(time.Time{}), timeNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return timeConv }},
	timeNType:         {hasLength | isNullable, "time", 4, reflect.TypeOf(time.Time{}), timeNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return timeConv }},
	smalldatetimeType: {isConcrete, "smalldatetime", 4, reflect.TypeOf(time.Time{}), datetimeNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return smallDateTimeConv }},
	datetimeNType:     {hasLength | isNullable, "datetime", 8, reflect.TypeOf(time.Time{}), datetimeNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return dateTimeConv }},
	dateNType:         {hasLength | isNullable, "datetime", 4, reflect.TypeOf(time.Time{}), datetimeNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return dateConv }},
	bigdatetimeType:   {hasLength | hasPrec | isConcrete, "bigdatetime", 8, reflect.TypeOf(time.Time{}), bigdatetimeNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return bigDateTimeConv }},
	bigdatetimeNType:  {hasLength | hasPrec | isConcrete | isNullable, "bigdatetime", 8, reflect.TypeOf(time.Time{}), bigdatetimeNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return bigDateTimeConv }},
	bigtimeType:       {hasLength | hasPrec | isConcrete, "bigtime", 8, reflect.TypeOf(time.Time{}), bigtimeNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return timeConv }},
	bigtimeNType:      {hasLength | hasPrec | isNullable, "bigtime", 8, reflect.TypeOf(time.Time{}), bigtimeNType, encodeDateTime, decodeDateTime, func(*colFmt) driver.ValueConverter { return timeConv }},
	decimalType: {isConcrete | hasLength | hasPrec | hasScale, "decimal", 0, reflect.TypeOf(Num{}), numericNType, encodeNumeric, decodeNumeric, func(f *colFmt) driver.ValueConverter {
		return &numConverter{precision: f.precision, scale: f.scale}
	}},
	decimalNType: {hasLength | hasPrec | hasScale, "decimal", 0, reflect.TypeOf(Num{}), numericNType, encodeNumeric, decodeNumeric, func(f *colFmt) driver.ValueConverter {
		return &numConverter{precision: f.precision, scale: f.scale}
	}},
	numericType: {isConcrete | hasLength | hasPrec | hasScale, "numeric", 0, reflect.TypeOf(Num{}), numericNType, encodeNumeric, decodeNumeric, func(f *colFmt) driver.ValueConverter {
		return &numConverter{precision: f.precision, scale: f.scale}
	}},
	numericNType: {hasLength | hasPrec | hasScale | isNullable, "numeric", 0, reflect.TypeOf(Num{}), numericNType, encodeNumeric, decodeNumeric, func(f *colFmt) driver.ValueConverter {
		return &numConverter{precision: f.precision, scale: f.scale}
	}},
	realType:      {isConcrete, "real", 4, reflect.TypeOf(float64(0)), floatNType, encodeReal, decodeReal, func(*colFmt) driver.ValueConverter { return float32Conv }},
	floatType:     {isConcrete, "float", 8, reflect.TypeOf(float64(0)), floatNType, encodeFloat, decodeFloat, func(*colFmt) driver.ValueConverter { return float64Conv }},
	floatNType:    {isNullable, "float", 0, reflect.TypeOf(float64(0)), floatNType, encodeFloat, decodeFloat, func(*colFmt) driver.ValueConverter { return float64Conv }},
	tinyintType:   {isConcrete, "tinyint", 1, reflect.TypeOf(int64(0)), intNType, encodeTinyint, decodeTinyint, func(*colFmt) driver.ValueConverter { return tinyIntConv }},
	smallintType:  {isConcrete, "smallint", 2, reflect.TypeOf(int64(0)), intNType, encodeSmallint, decodeSmallint, func(*colFmt) driver.ValueConverter { return smallIntConv }},
	intType:       {isConcrete, "int", 4, reflect.TypeOf(int64(0)), intNType, encodeInt, decodeInt, func(*colFmt) driver.ValueConverter { return intConv }},
	bigintType:    {isConcrete, "bigint", 8, reflect.TypeOf(int64(0)), intNType, encodeBigint, decodeBigint, func(*colFmt) driver.ValueConverter { return bigIntConv }},
	intNType:      {isNullable, "int", 4, reflect.TypeOf(int64(0)), intNType, encodeInt, decodeIntN, func(*colFmt) driver.ValueConverter { return nullIntConv }},
	uTinyintType:  {isConcrete, "unsigned tinyint", 1, reflect.TypeOf(int64(0)), uIntNType, encodeTinyint, decodeTinyint, func(*colFmt) driver.ValueConverter { return tinyIntConv }},
	uSmallintType: {isConcrete, "smallint", 2, reflect.TypeOf(int64(0)), uIntNType, encodeUSmallint, decodeUSmallint, func(*colFmt) driver.ValueConverter { return uSmallIntConv }},
	uIntType:      {isConcrete, "unsigned int", 4, reflect.TypeOf(int64(0)), uIntNType, encodeUInt, decodeUInt, func(*colFmt) driver.ValueConverter { return uIntConv }},
	uInt4Type:     {isConcrete, "unsigned int", 4, reflect.TypeOf(int64(0)), uIntNType, encodeUInt, decodeInt, func(*colFmt) driver.ValueConverter { return uIntConv }},
	uBigintType:   {isConcrete, "unsigned bigint", 8, reflect.TypeOf(uint64(0)), uIntNType, encodeUBigint, decodeUBigint, func(*colFmt) driver.ValueConverter { return uBigIntConv }},
	uIntNType:     {isNullable, "unsigned int", 4, reflect.TypeOf(int64(0)), uIntNType, encodeUInt, decodeUIntN, func(*colFmt) driver.ValueConverter { return nullIntConv }},
	moneyType: {isConcrete, "money", 8, reflect.TypeOf(Num{}), moneyNType, encodeMoney, decodeMoney, func(f *colFmt) driver.ValueConverter {
		return &numConverter{precision: 20, scale: 4}
	}},
	smallmoneyType: {isConcrete, "smallmoney", 4, reflect.TypeOf(Num{}), moneyNType, encodeSmallMoney, decodeSmallmoney, func(f *colFmt) driver.ValueConverter {
		return &numConverter{precision: 10, scale: 4}
	}},
	moneyNType: {isNullable, "money", 0, reflect.TypeOf(Num{}), moneyNType, encodeMoney, decodeMoney, func(f *colFmt) driver.ValueConverter {
		return &numConverter{precision: 20, scale: 4}
	}},
	textType:       {isConcrete | hasLength | isNullable | isLong | isLob, "text", 0, reflect.TypeOf(""), textType, encodeText, decodeText, charFct},
	imageType:      {isConcrete | hasLength | isNullable | isLong | isLob, "image", 0, reflect.TypeOf([]byte{}), imageType, encodeImage, decodeImage, byteFct},
	extendedType:   {hasLength | isNullable | isLong, "long", 0, nil, extendedType, nil, nil, byteFct},
	longBinaryType: {hasLength | isNullable | isLong, "binary", 0, reflect.TypeOf([]byte{}), longBinaryType, encodeLongbinary, decodeBinary, byteFct},
	unitextType:    {isConcrete | hasLength | isNullable | isLong | isLob, "unitext", 0, reflect.TypeOf(""), longBinaryType, encodeUnitext, decodeUnitext, charFct},
	unicharType:    {isConcrete | hasLength | isNullable | isLong, "unichar", 0, reflect.TypeOf(""), longBinaryType, encodeUniChar, decodeUniChar, charFct},
	univarcharType: {isConcrete | hasLength | isNullable | isLong, "unichar", 0, reflect.TypeOf(""), longBinaryType, encodeUniChar, decodeUniChar, charFct},
	longCharType:   {hasLength | isNullable | isLong | isConcrete, "varchar", 0, reflect.TypeOf(""), longCharType, encodeLongchar, decodeChar, charFct},
	varbinaryType:  {isConcrete | hasLength | isNullable | isConcrete, "varbinary", 0, reflect.TypeOf([]byte{}), varbinaryType, encodeBinary, decodeBinary, byteFct},
	varcharType:    {isConcrete | hasLength | isNullable | isConcrete, "varchar", 0, reflect.TypeOf(""), varcharType, encodeChar, decodeChar, charFct}}

//
// data binary.Encoders/decoders
//

// decodeDateTime decodes a time. See EncodeDateTime for more details
func decodeDateTime(e *binary.Encoder, i colType) (interface{}, error) {
	// sybase julian day
	var julianDay, y, m, d, ms, min, ns, s int
	var t time.Time

	realType := i.dataType

	if i.dataType == datetimeNType {
		switch i.bufferSize {
		default:
			return nil, fmt.Errorf("tds: invalid length (%d) for datatype %#x", i.bufferSize, i.dataType)
		case 4:
			realType = smalldatetimeType
		case 8:
			realType = datetimeType
		}
	}

	switch realType {
	default:
		return nil, fmt.Errorf("tds: unexpected data type: %s", realType)
	// datetime, julian day from sybase epoch and number of milliseconds since midnight
	case datetimeType:
		julianDay = int(e.Int32())
		ms = int(e.Int32()) * 1000 / 300
	// date, julian day from sybase epoch and number 300ms since midnight
	case dateType, dateNType:
		julianDay = int(e.Int32())
	// time, number 300ms since midnight
	case timeType:
		ms = int(e.Int32()) * 1000 / 300
	// smalldatetime, julian day from sybase epoch and number of minutes since midnight
	case smalldatetimeType:
		julianDay = int(e.Int16()) & 0xFFFF
		min = int(e.Int16())
	// BigTime, seconds since midnight and decimacroseconds
	case bigtimeType, bigtimeNType:
		bigTime := e.Uint64()
		ns = int(bigTime%1000000) * 1000
		s = int(bigTime/1000000) % 86400
	case bigdatetimeType, bigdatetimeNType:
		bigTime := e.Uint64()
		ns = int(bigTime%1000000) * 1000
		s = int(bigTime/1000000) % 86400
		julianDay = int(bigTime/1000000)/86400 - 693961
	}

	if julianDay != 0 {
		// Convert Sybase julian date (days since 1900-01-01) to date
		// Fliegel, H. F. & van Flandern, T. C. 1968, Communications of the ACM, 11, 657.
		l := julianDay + 68569 + 2415021
		n := 4 * l / 146097
		l = l - (146097*n+3)/4
		y = 4000 * (l + 1) / 1461001
		l = l - 1461*y/4 + 31
		m = 80 * l / 2447
		d = l - 2447*m/80
		l = m / 11
		m = m + 2 - 12*l
		y = 100*(n-49) + y + l
		t = time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.Local)
	} else {
		t = time.Date(1900, time.Month(1), 1, 0, 0, 0, 0, time.Local)
	}

	// finally add the offset
	if min != 0 {
		t = t.Add(time.Duration(min) * time.Minute)
	}
	if ms != 0 {
		t = t.Add(time.Duration(ms) * time.Millisecond)
	}
	if ns != 0 {
		t = t.Add(time.Duration(ns) * time.Nanosecond)
	}
	if s != 0 {
		t = t.Add(time.Duration(s) * time.Second)
	}

	return t, e.Err()
}

// encodeDateTime encodes a time.Time value to sybase datetime fields
func encodeDateTime(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(time.Time)
	if !ok {
		return errors.New("tds: invalid data type for date")
	}

	// The date to julian day since sybase epoch conversion uses the algorithm from
	// Fliegel, H. F. & van Flandern, T. C. 1968, Communications of the ACM, 11, 657.
	y, m, d := val.Year(), int(val.Month()), val.Day()
	julianDay := d - 2415021 - 32075 + 1461*(y+4800+(m-14)/12)/4 + 367*
		(m-2-(m-14)/12*12)/12 - 3*((y+4900+(m-14)/12)/100)/4

	// see decoding function for logic
	switch i.dataType {
	default:
		return fmt.Errorf("tds: unexpected data type: %s", i.dataType)
	case datetimeType, datetimeNType:
		e.WriteInt32(int32(julianDay))
		e.WriteInt32(int32(val.Hour()*1080000 + val.Minute()*18000 +
			val.Second()*300 + val.Nanosecond()/1000000))
	case dateType, dateNType:
		e.WriteInt32(int32(julianDay))
	case timeType, timeNType:
		e.WriteInt32(int32(val.Hour()*1080000 + val.Minute()*18000 +
			val.Second()*300 + val.Nanosecond()/1000000))
	case smalldatetimeType:
		e.WriteInt16(int16(julianDay))
		e.WriteInt16(int16(val.Hour()*60 + val.Minute()))
	case bigtimeType, bigtimeNType:
		e.WriteUint64(uint64((val.Hour()*3600+val.Minute()*60+val.Second()))*1000000 +
			uint64(val.Nanosecond())/1000)
	case bigdatetimeType, bigdatetimeNType:
		e.WriteUint64(uint64(julianDay+693961)*86400*1000000 +
			uint64((val.Hour()*3600+val.Minute()*60+val.Second()))*1000000 + uint64(val.Nanosecond())/1000)
	}
	err = e.Err()
	return err
}

func encodeBool(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(bool)
	if !ok {
		return errors.New("tds: invalid data type for Bit")
	}
	if val {
		e.WriteByte(byte(1))
	} else {
		e.WriteByte(byte(0))
	}
	err = e.Err()
	return err
}

func encodeTinyint(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(int64)
	if !ok {
		return errors.New("tds: invalid data type for tinyint")
	}
	e.WriteUint8(uint8(val))
	err = e.Err()
	return err
}

func encodeSmallint(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(int64)
	if !ok {
		return errors.New("tds: invalid data type for smallint")
	}
	e.WriteInt16(int16(val))
	err = e.Err()
	return err
}

func encodeInt(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(int64)
	if !ok {
		return errors.New("tds: invalid data type for int")
	}
	e.WriteInt32(int32(val))
	err = e.Err()
	return err
}

func encodeUInt(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(int64)
	if !ok {
		return errors.New("tds: invalid data type for unsigned int")
	}
	e.WriteUint32(uint32(val))
	err = e.Err()
	return err
}

func encodeBigint(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(int64)
	if !ok {
		return errors.New("tds: invalid data type for bigint")
	}
	e.WriteInt64(val)
	err = e.Err()
	return err
}

func encodeUBigint(e *binary.Encoder, s interface{}, i colType) (err error) {
	var i64 uint64
	// should get unsigned ints most of the time,
	// however values > MaxInt64 are converted as strings by the parameter converter
	val, ok := s.(int64)
	if !ok {
		str, ok := s.(string)
		if !ok {
			return errors.New("tds: invalid data type for UBigInt")
		}
		i64, err = strconv.ParseUint(str, 16, 64)
		if err != nil {
			return fmt.Errorf("tds: could not parse string to UBigInt: %s", err)
		}
	} else {
		i64 = uint64(val)
	}
	e.WriteUint64(i64)
	err = e.Err()
	return err
}

func encodeUSmallint(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(int64)
	if !ok {
		return errors.New("tds: invalid data type for Uint2")
	}
	e.WriteUint16(uint16(val))
	err = e.Err()
	return err
}

func encodeReal(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(float64)
	if !ok {
		return errors.New("tds: invalid data type for Real")
	}
	e.WriteUint32(math.Float32bits(float32(val)))
	err = e.Err()
	return err
}

func encodeFloat(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(float64)
	if !ok {
		return errors.New("tds: invalid data type for Flt8")
	}
	e.WriteUint64(math.Float64bits(val))
	err = e.Err()
	return err
}

func encodeChar(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(string)
	if !ok {
		return errors.New("tds: invalid data type for Varchar/Varchar")
	}
	e.WriteStringWithLen(8, val)
	err = e.Err()
	return err
}

func encodeBinary(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.([]byte)
	if !ok {
		return errors.New("tds: invalid data type for Varbinary")
	}
	e.WriteUint8(uint8(len(val)))
	e.Write(val[:])
	err = e.Err()
	return err
}

func encodeLongchar(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(string)
	if !ok {
		return errors.New("tds: invalid data type for Varchar/Varchar")
	}
	e.WriteStringWithLen(32, val)
	err = e.Err()
	return err
}

func encodeLongbinary(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.([]byte)
	if !ok {
		return errors.New("tds: invalid data type for Varbinary")
	}
	e.WriteUint32(uint32(len(val)))
	e.Write(val[:])
	err = e.Err()
	return err
}

func encodeImage(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.([]byte)
	if !ok {
		return errors.New("tds: invalid data type for Image")
	}
	// textptr len
	e.WriteInt8(16)
	// textprt (16) + timestamp (8)
	e.Pad(0xff, 24)
	e.WriteInt32(int32(len(val)))
	e.Write(val[:])
	err = e.Err()
	return err
}

var utf16Encoder = unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()

func encodeUniChar(e *binary.Encoder, s interface{}, i colType) error {
	val, ok := s.(string)
	if !ok {
		return errors.New("tds: invalid data type for unitext/unichar")
	}
	bytes, err := utf16Encoder.Bytes([]byte(val))
	if err != nil {
		return errors.New("tds: conversion to utf-16 failed")
	}
	e.WriteUint32(uint32(len(bytes)))
	e.Write(bytes[:])

	err = e.Err()
	return err
}

func encodeText(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(string)
	if !ok {
		return errors.New("tds: invalid data type for Text")
	}
	// textptr len
	e.WriteInt8(16)
	// textprt (16) + timestamp (8)
	e.Pad(0xff, 24)
	e.WriteInt32(int32(len([]byte(val))))
	e.WriteString(val)
	err = e.Err()
	return err
}

func encodeUnitext(e *binary.Encoder, s interface{}, i colType) (err error) {
	val, ok := s.(string)
	if !ok {
		return errors.New("tds: invalid data type for Text")
	}
	// textptr len
	e.WriteInt8(16)
	// textprt (16) + timestamp (8)
	e.Pad(0xff, 24)
	bytes, err := utf16Encoder.Bytes([]byte(val))
	if err != nil {
		return errors.New("tds: conversion to utf-16 failed")
	}
	e.WriteUint32(uint32(len(bytes)))
	e.Write(bytes[:])

	err = e.Err()
	return err
}

// wrappers around encode numeric to encode money.
// we need to fix type info here as it is not sent by the server
func encodeMoney(e *binary.Encoder, s interface{}, i colType) (err error) {
	i.precision, i.scale = 20, 4
	return encodeNumeric(e, s, i)
}

func encodeSmallMoney(e *binary.Encoder, s interface{}, i colType) (err error) {
	i.precision, i.scale = 10, 4
	return encodeNumeric(e, s, i)
}

func decodeBool(e *binary.Encoder, i colType) (interface{}, error) {
	out := e.Int8() == 1
	err := e.Err()
	return out, err
}

func decodeSmallmoney(e *binary.Encoder, i colType) (interface{}, error) {
	intVal := int64(e.Int32())

	// cast as a big.Rat
	out := new(big.Rat).SetFrac(new(big.Int).SetInt64(intVal), decimalPowers[4])

	// return as numeric
	return Num{r: *out, precision: 10, scale: 4}, nil
}

func decodeMoney(e *binary.Encoder, i colType) (interface{}, error) {
	hi := e.Uint32()
	lo := e.Uint32()
	if err := e.Err(); err != nil {
		return nil, err
	}

	intVal := int64(int64(hi)<<32 + int64(lo&0xffffffff))
	// cast as a big.Rat
	out := new(big.Rat).SetFrac(new(big.Int).SetInt64(intVal), decimalPowers[4])

	// return as numeric
	return Num{r: *out, precision: 19, scale: 4}, nil
}

var utf16Decoder = unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder()

func decodeUniChar(e *binary.Encoder, i colType) (interface{}, error) {
	bytes := make([]byte, i.bufferSize)
	e.Read(bytes)
	err := e.Err()
	bytes, err = utf16Decoder.Bytes(bytes)
	return string(bytes), err
}

func decodeTinyint(e *binary.Encoder, i colType) (interface{}, error) {
	out := int64(e.Uint8())
	err := e.Err()
	return out, err
}

func decodeSmallint(e *binary.Encoder, i colType) (interface{}, error) {
	out := int64(e.Int16())
	err := e.Err()
	return out, err
}

func decodeUSmallint(e *binary.Encoder, i colType) (interface{}, error) {
	out := int64(e.Uint16())
	err := e.Err()
	return out, err
}

func decodeInt(e *binary.Encoder, i colType) (interface{}, error) {
	out := int64(e.Int32())
	err := e.Err()
	return out, err
}

func decodeUInt(e *binary.Encoder, i colType) (interface{}, error) {
	out := int64(e.Uint32())
	err := e.Err()
	return out, err
}

func decodeIntN(e *binary.Encoder, i colType) (interface{}, error) {
	var out int64
	switch i.bufferSize {
	default:
		return out, errors.New("tds: invalid data size for UintN")
	case 2:
		out = int64(e.Int16())
	case 4:
		out = int64(e.Int32())
	case 8:
		out = int64(e.Int64())
	}
	err := e.Err()
	return out, err
}

func decodeUIntN(e *binary.Encoder, i colType) (interface{}, error) {
	var out interface{}
	switch i.bufferSize {
	default:
		return out, errors.New("tds: invalid data size for UintN")
	case 2:
		out = int64(e.Uint16())
	case 4:
		out = int64(e.Uint32())
	case 8:
		out = uint64(e.Uint64())
	}
	err := e.Err()
	return out, err
}

func decodeBigint(e *binary.Encoder, i colType) (interface{}, error) {
	out := int64(e.Int64())
	err := e.Err()
	return out, err
}

func decodeUBigint(e *binary.Encoder, i colType) (interface{}, error) {
	out := uint64(e.Uint64())
	err := e.Err()
	return out, err
}

func decodeReal(e *binary.Encoder, i colType) (interface{}, error) {
	out := math.Float32frombits(e.Uint32())
	err := e.Err()
	return out, err
}

func decodeFloat(e *binary.Encoder, i colType) (interface{}, error) {
	out := math.Float64frombits(e.Uint64())
	err := e.Err()
	return out, err
}

func decodeChar(e *binary.Encoder, i colType) (interface{}, error) {
	out := e.ReadStringWithLen(int(i.bufferSize))
	err := e.Err()
	return out, err
}

func decodeBinary(e *binary.Encoder, i colType) (interface{}, error) {
	out := make([]byte, i.bufferSize)
	e.Read(out)
	err := e.Err()
	return out, err
}

func decodeImage(e *binary.Encoder, i colType) (interface{}, error) {
	e.Skip(24)
	var out []byte

	// field length
	len := e.Int32()
	if len != 0 {
		out = make([]byte, len)
		e.Read(out)
	}
	err := e.Err()
	return out, err
}

func decodeText(e *binary.Encoder, i colType) (interface{}, error) {
	e.Skip(24)
	out, len := e.ReadString(32)
	err := e.Err()
	if len == 0 {
		return nil, err
	}
	return out, err
}

func decodeUnitext(e *binary.Encoder, i colType) (interface{}, error) {
	e.Skip(24)
	var out []byte
	var err error

	// field length
	len := e.Int32()

	if len != 0 {
		out = make([]byte, len)
		e.Read(out)
	} else {
		err = e.Err()
		return nil, err
	}
	out, err = utf16Decoder.Bytes(out)
	if err != nil {
		return nil, errors.New("tds: conversion from utf-16 failed")
	}

	err = e.Err()
	return string(out), err
}

//
// data converters
//

// ErrOverFlow is raised when there is an overflow when converting
// the parameter to	 a database type.
var ErrOverFlow = errors.New("overflow when converting to database type")

// ErrBadType is raised when trying to convert a value to an incompatible data type.
var ErrBadType = errors.New("invalid type given")

// ErrNonNullable is raised when trying to insert null values to non-null fields
var ErrNonNullable = errors.New("trying to insert null values into non null column")

// typeCheckConverter just run type checks
type typeCheckConverter struct {
	expectedType reflect.Type
}

func (tc typeCheckConverter) ConvertValue(src interface{}) (driver.Value, error) {
	// no nil checks for now
	if src == nil {
		return nil, nil
	}
	if reflect.TypeOf(src) != tc.expectedType {
		return nil, ErrBadType
	}
	return src, nil
}

// bitConverter checks the data type and ensures no null values are sent
type boolConverter struct{}

func (b boolConverter) ConvertValue(src interface{}) (driver.Value, error) {
	if src == nil {
		return nil, ErrNonNullable
	}

	if _, ok := src.(bool); !ok {
		return nil, ErrBadType
	}
	return src, nil
}

// dateConverter just checks for overflows
// Right now you can only give time.Time and *time.Time parameters
type dateConverter struct {
	min     time.Time
	max     time.Time
	hasTime bool
	hasDate bool
}

func (d dateConverter) ConvertValue(src interface{}) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	var val time.Time
	switch src.(type) {
	default:
		return nil, ErrBadType
	case *time.Time:
		val = *src.(*time.Time)
	case time.Time:
		val = src.(time.Time)
	}

	// no date, no range check
	if !d.hasDate {
		return val, nil
	}

	if val.After(d.max) || val.Before(d.min) {
		return nil, ErrOverFlow
	}

	return val, nil
}

// intConverter checks for overflows
// and eventually convert to unsigned int
type intConverter struct {
	min int64
	max uint64
}

func (i intConverter) ConvertValue(src interface{}) (driver.Value, error) {
	// fast path first
	if i64, ok := src.(int64); ok {
		// overflow check
		if i64 < i.min || (i64 > 0 && uint64(i64) > i.max) {
			return nil, ErrOverFlow
		}
		return i64, nil
	}

	if src == nil {
		return nil, nil
	}

	var i64 int64
	var u64 uint64

	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	default:
		return nil, ErrBadType
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, nil
		}
		return i.ConvertValue(rv.Elem().Interface())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		i64 = rv.Int()
		// overflow check
		if i64 < i.min || i64 > int64(i.max) {
			return nil, ErrOverFlow
		}
		return i64, nil
	case reflect.String:
		i, err := strconv.Atoi(rv.String())
		if err != nil {
			return nil, ErrBadType
		}
		return int64(i), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64 = rv.Uint()
	}

	// overflow check (unsigned)
	if u64 > i.max {
		return nil, ErrOverFlow
	}

	// return unsigned ints as hex chars when > 2^63
	if u64 > math.MaxInt64 {
		return strconv.FormatUint(u64, 16), nil
	}
	return int64(u64), nil
}

// floatConverter checks for overflows
// and converts to float64
type floatConverter struct {
	max float64
}

func (f floatConverter) ConvertValue(src interface{}) (driver.Value, error) {
	var f64 float64

	// fast path first
	if f64, ok := src.(float64); ok {
		// overflow check
		if math.Abs(f64) > f.max {
			return nil, ErrOverFlow
		}
		return f64, nil
	}

	if src == nil {
		return nil, nil
	}

	// cast first
	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	default:
		return nil, ErrBadType
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, nil
		}
		return f.ConvertValue(rv.Elem().Interface())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		f64 = float64(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		f64 = float64(rv.Uint())
	case reflect.Float64, reflect.Float32:
		f64 = rv.Float()
	}

	// overflow check
	if math.Abs(f64) > f.max {
		return nil, ErrOverFlow
	}

	return f64, nil
}
