package gocaskDB

import (
	"errors"
	"path/filepath"
	"sync"
	"util"
)

const (
	// for error messages
	ErrNotOpen     = "Open database first. Use db.Open(). "
	ErrNotClosed   = "Database has been open, close first. Use db.Close()."
	ErrNotFound    = "Record not found."
	ErrCheckFailed = "Data check failed, db file may be damaged."
)

const (
	// for macros
	MergeNone = iota
	MergeAuto
)

type DB struct {
	options        DBOptions
	rwlock         *sync.RWMutex

	//infoFile       *os.File           // Main db file including db infos.
	//activeDBFile   *os.File           // Current db file to write in.
	//activeHintFile *os.File           // Current hint file to write in.
	//dbFiles        map[int32]*os.File // All db files.
	//dbPath         string             // Directory of db.
	//dbinfo         *DBinfo
	fileMgr *FileMgr
	hashtable      map[Key]*hashBody
	open           bool
}

type DBOptions struct {
	FILE_MAX   int32 // 10MB by default, no more than 2GB
	KEY_MAX    int32 // 1KB by default
	VAL_MAX    int32 // 65536B by default
	READ_CHECK bool  // false by default
	BUFFER_MAX int32 // 1MB by default
	CACHE_MAX  int32 // 1MB by default
}

var defaultOptions = DBOptions{
	10 << 20,
	1 << 10,
	1 << 16,
	false,
	1 << 20,
	1 << 20}

type Key string
type Value string

// unused
type Record struct {
	key   Key
	value Value
}

func GetDefaultOption() DBOptions {
	return defaultOptions
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
	db.fileMgr = new(FileMgr)
	db.fileMgr.dbPath = filepath.Dir(filename)
	db.fileMgr.dealingNewLock = new(sync.Mutex)
	err := db.fileMgr.OpenAllFile(filename)
	if err != nil {
		return err
	}
	db.hashtable, err = db.fileMgr.RebuildHashFromHint()
	if err != nil {
		return err
	}
	db.open = true
	//fmt.Println(db.hashtable)
	return nil
}

func (db *DB) Close() error {
	db.open = false
	if err := db.fileMgr.CloseAll(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Get(key Key) (value Value, err error) {
	if !db.open {
		return Value(0), errors.New(ErrNotOpen)
	}
	db.rlock(key)
	defer db.runlock(key)
	if db.options.READ_CHECK {
		return db.readAndCheck(key)
	}
	return db.read(key)
}

func (db *DB) Set(key Key, value Value) error {
	if !db.open {
		return errors.New(ErrNotOpen)
	}
	data := wrap(key, value, false) // wrap data into DataPackage
	db.lock(key)
	defer db.unlock(key)
	return db.write(data)
}

func (db *DB) Delete(key Key) error {
	if !db.open {
		return errors.New(ErrNotOpen)
	}
	data := wrap(key, Value(0), true)
	db.lock(key)
	defer db.unlock(key)
	return db.write(data)
}

/* 	Type of callback function for async get/set/delete,
 *	function will be called once the get/set/delete
 *	finished or failed. (err==nil) means a success and
 *	the value of a query will be in (value) if the
 *	callback is for a Get operation.
 */
type Callback func(err error, value Value)

func (db *DB) GetAsync(key Key, callback Callback) {
	go func() {
		val, err := db.Get(key)
		callback(err, val)
	}()
}

func (db *DB) SetAsync(key Key, value Value, callback Callback) {
	go func() {
		err := db.Set(key, value)
		callback(err, Value(0))
	}()
}

func (db *DB) DeleteAsync(key Key, callback Callback) {
	go func() {
		err := db.Delete(key)
		callback(err, Value(0))
	}()
}

func (db *DB) write(packet *DataPacket) error {
	// TODO: prepared for lock of io, useless for now ...?

	// write files (.gcdb, .gch)
	hbody, err := db.fileMgr.WriteDataToFile(packet, db.options)
	if err != nil {
		return err
	}

	// unlock of io, useless for now

	// write hash table
	db.WriteHashTable(packet.key, hbody)

	return nil
}

func (db *DB) read(key Key) (Value, error) {
	// read hashtable to get value position
	hbody, ok := db.ReadHashTable(key)
	if !ok {
		return Value(0), errors.New(ErrNotFound)
	}

	// read value
	value, err := db.fileMgr.ReadValueFromFile(hbody.fileno, hbody.vpos, hbody.vsz)
	if err != nil {
		return Value(0), err
	}
	return value, nil
}

func (db *DB) readAndCheck(key Key) (Value, error) {
	// read hashtable to get value position
	hbody, ok := db.ReadHashTable(key)
	if !ok {
		return Value(0), errors.New(ErrNotFound)
	}

	// read record (DataPacket)
	dp, err := db.fileMgr.ReadRecordFromFile(hbody.fileno, hbody.vpos, util.Sizeof(string(key)), hbody.vsz)
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
