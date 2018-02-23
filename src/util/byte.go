package util

import (
	"encoding/binary"
)

func Sizeof(x interface{}) int32 {
	s := int32(binary.Size(x))
	if s == -1 {
		return int32(binary.Size(ToBytes(x)))
	}
	return s
	//return int32(unsafe.Sizeof(x));
}

func ToBytes(x interface{}) []byte {

	switch x := x.(type) {
	case string:
		return []byte(x)
	case uint32:
		b := make([]byte, 4, 4)
		binary.LittleEndian.PutUint32(b, x)
		return b
	case int32:
		b := make([]byte, 4, 4)
		binary.LittleEndian.PutUint32(b, uint32(x))
		return b
	case uint64:
		b := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(b, x)
		return b
	case int64:
		b := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(b, uint64(x))
		return b
	default:
		return make([]byte, 0, 0)
	}
}
