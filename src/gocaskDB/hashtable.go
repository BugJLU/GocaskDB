package gocaskDB

import "os"

type hashBody struct {
	file *os.File
	vsz int32
	vpos int32
	timestamp int64
}

func WriteHashTable(key Key, hbody *hashBody, db *DB) {
	if hbody.vsz == -1 {	// if delete
		delete(db.hashtable, key)
	} else {	// if set
		db.hashtable[key] = hbody
	}
}

func RebuildHashFromHint(db *DB) map[Key]*hashBody {
	return make(map[Key]*hashBody)
}