package gocaskDB

import (
	"os"
	"io"
)

type Iterator interface {
	Next() (data interface{}, ok bool)
}

type HintData struct {
	timestamp int64
	ksz int32
	vsz int32
	vpos int32
	key Key
}

type HintIterator struct {
	hintFile *os.File
}

func GetHintIterator(hintFile *os.File) *HintIterator {
	hi := &HintIterator{hintFile}
	hi.hintFile.Seek(0, io.SeekStart)
	return hi
}

func (hi *HintIterator) Next() (data interface{}, ok bool) {
	hdata, err := ReadRecordFromHint(hi.hintFile)
	if err != nil {
		return nil, false
	}
	return hdata, true
}