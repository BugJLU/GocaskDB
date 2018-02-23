package gocaskDB

import (
	"util"
	"encoding/binary"
	"fmt"
)

//todo:test
func TestWrap()  {
	dp := wrap("abc我", "abcdefg", false)
	fmt.Println(dp.getBytes())
	//fmt.Println(dp.getHintBytes(0))
	var bb []byte
	t:=dp.getBytes()
	for i := range t {
		bb = append(bb, t[i]...)
	}
	dp1 := bytesToData(bb)
	fmt.Println(dp1.Check())
}

// A packet of data to be stored in db file on disk.
type DataPacket struct {
	crc       uint32
	timestamp int64
	ksz       int32
	vsz       int32
	key       Key
	value     Value
}

// Convert data into packet.
func wrap(key Key, value Value, del bool) *DataPacket {
	datapkt := DataPacket{
		0,
		util.UnixNano(),
		util.Sizeof(string(key)),
		util.Sizeof(string(value)),
		key,
		value}
	if del {
		datapkt.vsz = -1
	}
	datapart := datapkt.getDatapartBytes();
	var bytes []byte
	for b := range datapart {
		bytes = append(bytes, datapart[b]...)
	}
	datapkt.crc = util.Crc(bytes)
	return &datapkt
}

// Convert bytes to packet.
func bytesToData(bytes []byte) *DataPacket {
	data := new(DataPacket)
	b, bytes := bytes[:4], bytes[4:]
	data.crc = binary.LittleEndian.Uint32(b)
	b, bytes = bytes[:8], bytes[8:]
	data.timestamp = int64(binary.LittleEndian.Uint64(b))
	b, bytes = bytes[:4], bytes[4:]
	data.ksz = int32(binary.LittleEndian.Uint32(b))
	b, bytes = bytes[:4], bytes[4:]
	data.vsz = int32(binary.LittleEndian.Uint32(b))
	b, bytes = bytes[:data.ksz], bytes[data.ksz:]
	data.key = Key(b)
	b, bytes = bytes[:data.vsz], bytes[data.vsz:]
	data.value = Value(b)
	// TODO: check length of the raw bytes
	return data
}

func (datapkt *DataPacket)getDatapartBytes() [][]byte {
	var tmp []byte
	var datapart [][]byte
	tmp = util.ToBytes(datapkt.timestamp)
	datapart = append(datapart, tmp)
	tmp = util.ToBytes(datapkt.ksz)
	datapart = append(datapart, tmp)
	tmp = util.ToBytes(datapkt.vsz)
	datapart = append(datapart, tmp)
	datapart = append(datapart, []byte(datapkt.key))
	datapart = append(datapart, []byte(datapkt.value))
	return datapart
}

//	crc	| tstmp	|  ksz	|  vsz	|  key	|  val
func (datapkt *DataPacket)getBytes() [][]byte {
	dpb := datapkt.getDatapartBytes()
	crcb := util.ToBytes(datapkt.crc)
	result := make([][]byte, 1, 1)
	result[0] = crcb
	result = append(result, dpb...)
	return result
}

//func (datapkt *DataPacket)getHintBytes(vpos int32) [][]byte {
//	// TODO: hint bytes
//	return nil
//}

func (datapkt *DataPacket)Check() bool {
	data := datapkt.getDatapartBytes()
	var b []byte
	for i := range data {
		b = append(b, data[i]...)
	}
	if crc := util.Crc(b); crc == datapkt.crc {
		return true
	}
	return false
}