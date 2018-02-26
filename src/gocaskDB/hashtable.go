package gocaskDB

import (
	"os"
	"sort"
)

type hashBody struct {
	file      *os.File
	vsz       int32
	vpos      int32
	timestamp int64
}

func WriteHashTable(key Key, hbody *hashBody, db *DB) {
	if hbody.vsz == -1 { // if delete
		delete(db.hashtable, key)
	} else { // if set
		db.hashtable[key] = hbody
	}
}

func RebuildHashFromHint(db *DB) (map[Key]*hashBody, error) {
	hashtable := make(map[Key]*hashBody)

	// open hint files
	hintfiles, err := OpenAllHintFiles(db.dbinfo.Serial, db)
	if err != nil {
		return nil, err
	}

	// copy serial array and sort in descending order
	sarr := make([]int, len(db.dbinfo.Serial))
	for i := range db.dbinfo.Serial {
		sarr[i] = int(db.dbinfo.Serial[i])
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sarr)))

	// Traversal of all hint files.
	for i := range sarr {
		currentFile := hintfiles[int32(sarr[i])]
		itr := GetHintIterator(currentFile)

		// Traversal of all records in a hint file.
		for {
			val, ok := itr.Next()
			if !ok {
				break
			}
			hdata := val.(*HintData)
			hashbody := new(hashBody)
			hashbody.file = db.dbFiles[int32(sarr[i])]
			hashbody.vsz = hdata.vsz
			hashbody.vpos = hdata.vpos
			hashbody.timestamp = hdata.timestamp
			if oldbody, ok := hashtable[hdata.key]; !ok || oldbody.timestamp < hashbody.timestamp {
				if hashbody.vsz == -1 {
					delete(hashtable, hdata.key)
				} else {
					hashtable[hdata.key] = hashbody
				}
			}
		}
	}

	return hashtable, nil
}
