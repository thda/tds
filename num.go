package tds

import (
	"database/sql/driver"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"sync"

	"github.com/thda/tds/binary"

	"errors"
)

// indicates the number of bytes needed to encode a numeric
// according to the precision (index of the table)
var numericBytes = []int{1,
	2, 2, 3, 3, 4, 4, 4, 5, 5,
	6, 6, 6, 7, 7, 8, 8, 9, 9, 9,
	10, 10, 11, 11, 11, 12, 12, 13, 13, 14,
	14, 14, 15, 15, 16, 16, 16, 17, 17, 18,
	18, 19, 19, 19, 20, 20, 21, 21, 21, 22,
	22, 23, 23, 24, 24, 24, 25, 25, 26, 26,
	26, 27, 27, 28, 28, 28, 29, 29, 30, 30,
	31, 31, 31, 32, 32, 33, 33, 33}

// implement the numeric/nullnumeric data types, and provide Scanner/Valuer for it
// as well as serialization/deserialization methods.

// Num represents a sybase numeric data type
type Num struct {
	r         big.Rat
	precision int8
	scale     int8
	isNull    bool
}

// initialise all the possible exponents for numeric datatypes
// Will be mainly used to check for overflow.
var decimalPowers [38]*big.Int

func init() {
	for i := int64(0); i < 38; i++ {
		decimalPowers[i] = new(big.Int).Exp(big.NewInt(10), big.NewInt(i), nil)
	}
}

//
// Pools for performance
//

// big.Rat free list
var rPool = sync.Pool{
	New: func() interface{} { return new(big.Rat) },
}

// numm free list
var numPool = sync.Pool{
	New: func() interface{} { return new(Num) },
}

//
// Scanner and Valuer to satisfy database/sql interfaces
//

// Scan implements the Scanner interface.
// Allows initiating a tds.Num from a string, or any golang numeric type.
// When providing a string, it must be in decimal form,
// with an optional sign, ie -50.40
// The dot is the separator.
//
// Example:
//
//	num := Num{precision: p, scale: s}
//	num.Scan("-10.4")
//
// A loss of precision should alway cause an error (except for bugs, of course).
func (n *Num) Scan(src interface{}) error {
	// use string as an intermediate
	var strVal string
	var ok bool

	if strVal, ok = src.(string); !ok {
		if src == nil {
			n.isNull = true
			return nil
		}
		rv := reflect.ValueOf(src)
		switch rv.Kind() {
		default:
			return errors.New("unexpected type for numeric scan")
		case reflect.Ptr:
			if rv.IsNil() {
				n.isNull = true
				return nil
			}
			return n.Scan(rv.Elem().Interface())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			strVal = strconv.FormatInt(rv.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			strVal = strconv.FormatUint(rv.Uint(), 10)
		case reflect.Float64:
			strVal = strconv.FormatFloat(rv.Float(), 'g', -1, 64)
		case reflect.Float32:
			strVal = strconv.FormatFloat(rv.Float(), 'g', -1, 32)
		}
	}

	if _, ok = n.r.SetString(strVal); !ok {
		return fmt.Errorf("tds: could not parse string %s to number", strVal)
	}

	// check for loss of precision
	mul := rPool.Get().(*big.Rat).SetInt(decimalPowers[n.scale])
	mul.Mul(mul, &n.r)
	if !mul.IsInt() {
		return ErrOverFlow
	}
	return nil
}

// implement the stringer interface
func (n Num) String() string {
	// shortcuts for ints
	if n.r.IsInt() {
		b := []byte(n.r.String())
		return string(b[:len(b)-2])
	}

	mul := rPool.Get().(*big.Rat).SetInt(decimalPowers[n.scale])
	mul.Mul(&n.r, mul)
	defer rPool.Put(mul)

	if !mul.IsInt() {
		return "incorrect rational"
	}
	b := []byte(mul.String())
	// TODO: remove trailing zeros
	return string(b[:len(b)-2-int(n.scale)]) + "." + string(b[len(b)-2-int(n.scale):len(b)-2])
}

// Rat returns the underlying big.Rat value
func (n Num) Rat() big.Rat {
	return n.r
}

// numConverter just checks for overflows
// Right now you can only give time.Time and *time.Time parameters
type numConverter struct {
	precision int8
	scale     int8
}

// ConvertValue will convert to an array of bytes, the first two being the precision and scale
func (nc numConverter) ConvertValue(src interface{}) (driver.Value, error) {
	var err error

	if src == nil {
		return nil, nil
	}

	// is numeric?
	if num, ok := src.(Num); ok {
		if num.isNull {
			return nil, nil
		}

		// check for loss of precision
		if num.precision > nc.precision || num.scale > nc.scale {
			return nil, ErrOverFlow
		}

		return []byte(num.String()), err
	}

	// get num from pool
	num := numPool.Get().(*Num)
	defer numPool.Put(num)
	num.precision, num.scale, num.isNull = nc.precision, nc.scale, false

	// check for driver values
	if val, ok := src.(driver.Valuer); ok {
		if src, err = val.Value(); err != nil {
			return nil, err
		}
		if src == nil {
			return nil, nil
		}
	}

	// use scan to convert to numeric
	if err = num.Scan(src); err != nil {
		return nil, err
	}

	return []byte(num.String()), err
}

//
// Encoding routines.
//

// encodeNumeric encodes an array of bytes, given by numConverter, to a numeric.
// We expect the precition to be checked at Scan/Value time.
// Money/smallmoney fields are handled here as they are indeed numeric in disguise
func encodeNumeric(e *binary.Encoder, s interface{}, i colType) (err error) {
	bytes, ok := s.([]byte)
	if !ok {
		return errors.New("invalid data type for numeric")
	}

	num := numPool.Get().(*Num)
	defer numPool.Put(num)

	num.precision, num.scale = i.precision, i.scale
	err = num.Scan(string(bytes[:]))
	if err != nil {
		return fmt.Errorf("tds: error while scanning array of bytes to numeric: %s", err)
	}

	// Multiply by the scale before serializing
	mul := rPool.Get().(*big.Rat).SetInt(decimalPowers[i.scale])
	num.r.Mul(&num.r, mul)
	defer rPool.Put(mul)

	// no loss of precision will be tolerated.
	if !num.r.IsInt() {
		return ErrOverFlow
	}

	// write to the wire as money of numeric, depending on data type
	switch i.dataType {
	case smallmoneyType:
		intVal, _ := num.r.Float64()
		e.WriteInt32(int32(intVal))
	case moneyNType, moneyType:
		intVal := num.r.Num().Int64()
		e.WriteUint32(uint32(intVal >> 32))
		e.WriteInt32(int32(intVal))
	case decimalType, numericType, decimalNType, numericNType:
		// length
		arraySize := numericBytes[int(i.precision)]
		e.WriteInt8(int8(arraySize))

		// sign
		if num.r.Sign() >= 0 {
			e.WriteByte(0x00)
		} else {
			e.WriteByte(0x01)
		}

		// left pad the values with 0s, then write data
		e.Pad(0x00, arraySize-int(num.r.Num().BitLen()/8)-2)
		e.Write(num.r.Num().Bytes())
	}
	err = e.Err()
	return err
}

// decodeNumeric decodes a numeric from the wire.
// Returns a big.Rat
func decodeNumeric(e *binary.Encoder, i colType) (interface{}, error) {
	sign := e.Int8()

	// read all the bytes
	bytes := make([]byte, i.bufferSize-1)
	e.Read(bytes)

	// cast as a big.Rat
	out := new(big.Rat).SetFrac(new(big.Int).SetBytes(bytes), decimalPowers[i.scale])
	if sign != 0 {
		out = out.Neg(out)
	}
	return Num{r: *out, precision: i.precision, scale: i.scale}, e.Err()
}
