package gocaskDB

import (
	"sync"
	"os"
	"errors"
	"path/filepath"
	"util"
)

const (
	// for error messages
	ErrNotOpen = "Open database first. Use db.Open(). "
	ErrNotClosed = "Database has been open, close first. Use db.Close()."
	ErrNotFound = "Record not found."
	ErrCheckFailed = "Data check failed, db file may be damaged."
)

const (
	// for macros
	MergeNone = iota
	MergeAuto
)

type DB struct {
	options DBOptions
	rwlock *sync.RWMutex
	dealingLock *sync.Mutex
	infoFile *os.File	// Main db file including db infos.
	activeDBFile *os.File	// Current db file to write in.
	activeHintFile *os.File	// Current hint file to write in.
	dbFiles map[int32]*os.File	// All db files.
	dbPath string // Directory of db.
	dbinfo *DBinfo
	hashtable map[Key]*hashBody
	open bool
}

type DBOptions struct {
	file_max int32	// 10MB by default, no more than 2GB
	key_max int32	// 1KB by default
	val_max int32	// 65536B by default
	read_check bool	// false by default
}

var defaultOptions = DBOptions{ 10<<20, 1<<10, 1<<16, false }

type Key string;
type Value string;

// unused
type Record struct {
	key Key;
	value Value
}

func (db *DB) Open(filename string) error {
	return db.OpenWithOptions(filename, defaultOptions)
}

func (db *DB) OpenWithOptions(filename string, options DBOptions) error {
	if db.open {
		return errors.New(ErrNotClosed)
	}
	db.options = options
	db.rwlock = new(sync.RWMutex)
	db.dealingLock = new(sync.Mutex)
	db.dbPath = filepath.Dir(filename)
	//db.hashtable = make(map[Key]*hashBody)
	err := OpenAllFile(filename, db)
	if err!=nil {
		return err
	}
	db.hashtable = RebuildHashFromHint(db)
	db.open = true
	return nil
}

func (db *DB) Close() error {
	db.open = false
	if err := db.activeDBFile.Close(); err != nil {
		return err
	}
	if err := db.activeHintFile.Close(); err != nil {
		return err
	}
	if err := db.infoFile.Close(); err != nil {
		return err
	}
	for i := range db.dbFiles{
		if err:= db.dbFiles[i].Close(); err != nil {
			return err
		}
	}
	return nil;
}

func (db *DB) Get(key Key) (value Value, err error) {
	if !db.open {
		return Value(0), errors.New(ErrNotOpen)
	}
	db.rlock(key)
	defer db.runlock(key)
	if db.options.read_check {
		return db.readAndCheck(key)
	}
	return db.read(key);
}

func (db *DB) Set(key Key, value Value) error {
	if !db.open {
		return errors.New(ErrNotOpen)
	}
	data := wrap(key, value, false)	// wrap data into DataPackage
	db.lock(key)
	defer db.unlock(key)
	return db.write(data);
}

func (db *DB) Delete(key Key) error {
	if !db.open {
		return errors.New(ErrNotOpen)
	}
	data := wrap(key, Value(0), true)
	db.lock(key)
	defer db.unlock(key)
	return db.write(data);
}

/* 	Type of callback function for async get/set/delete,
 *	function will be called once the get/set/delete
 *	finished or failed. (err==nil) means a success and
 *	the value of a query will be in (value) if the
 *	callback is for a Get operation.
 */
type Callback func(err error, value Value);

func (db *DB) GetAsync(key Key, callback Callback) {
	go func() {
		val, err := db.Get(key);
		callback(err, val);
	}()
}

func (db *DB) SetAsync(key Key, value Value, callback Callback) {
	go func() {
		err := db.Set(key, value);
		callback(err, Value(0));
	}()
}

func (db *DB) DeleteAsync(key Key, callback Callback) {
	go func() {
		err := db.Delete(key);
		callback(err, Value(0));
	}()
}

func (db *DB) write(packet *DataPacket) error {
	// TODO: prepared for lock of io, useless for now ...?

	// write files (.gcdb, .gch)
	hbody, err := WriteData(packet, db)
	if err != nil {
		return err
	}

	// unlock of io, useless for now

	// write hash table
	WriteHashTable(packet.key, hbody, db)

	return nil
}


func (db *DB) read(key Key) (Value, error) {
	// read hashtable to get value position
	hbody, ok := db.hashtable[key]
	if !ok {	// If key does not exist or has been removed.
		return Value(0), errors.New(ErrNotFound)
	}

	// read value
	value, err := ReadValueFromFile(hbody.file, hbody.vpos, hbody.vsz)
	if err != nil {
		return Value(0), err
	}
	return value, nil
}

func (db *DB) readAndCheck(key Key) (Value, error) {
	// read hashtable to get value position
	hbody, ok := db.hashtable[key]
	if !ok {	// If key does not exist or has been removed.
		return Value(0), errors.New(ErrNotFound)
	}

	// read record (DataPacket)
	dp, err := ReadRecordFromFile(hbody.file, hbody.vpos, util.Sizeof(string(key)), hbody.vsz)
	if err != nil {
		return Value(0), err
	}

	// check record
	if !dp.Check() {
		return dp.value, errors.New(ErrCheckFailed)
	}
	return dp.value, nil
}

func (db *DB) lock(key Key) {
	db.rwlock.Lock()

	/* 	Lock then unlock the dealingLock in
	 *	case that a goroutine is running to
	 *	create new active db and hint files.
	 */
	db.dealingLock.Lock()
	db.dealingLock.Unlock()
}

func (db *DB) unlock(key Key) {
	db.rwlock.Unlock()
}

func (db *DB) rlock(key Key) {
	db.rwlock.RLock()
}

func (db *DB) runlock(key Key) {
	db.rwlock.RUnlock()
}

// Iterator?

