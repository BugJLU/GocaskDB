package gocaskDB

type hashBody struct {
	//file      *os.File
	fileno	int32
	vsz       int32
	vpos      int32
	timestamp int64
}

func (db *DB)WriteHashTable(key Key, hbody *hashBody) {
	if hbody.vsz == -1 { // if delete
		delete(db.hashtable, key)
	} else { // if set
		db.hashtable[key] = hbody
	}
}

func (db *DB)ReadHashTable(key Key) (*hashBody, bool) {
	hbody, ok := db.hashtable[key]
	if !ok || hbody.vsz == -1 { // If key does not exist or has been removed.
		return nil, false
	}
	return hbody, true;
}


