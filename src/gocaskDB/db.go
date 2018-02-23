package gocaskDB

import (
	"fmt"
	"sync"
	"os"
	"errors"
	//"util"
	"path/filepath"
)

const (
	// for error messages
	ErrNotOpen = "Open database first. Use db.Open(). "
	ErrNotClosed = "Database has been open, close first. Use db.Close()."
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
	dbFiles []*os.File	// All db files.
	dbPath string // Directory of db.
	dbinfo *DBinfo
	hashtable map[Key]hashBody
	open bool
}

type DBOptions struct {
	file_max int32	// 10MB by default
	key_max int32	// 1KB by default
	val_max int32	// 65536B by default

}

var defaultOptions = DBOptions{ 10<<20, 1<<10, 1<<16 }

type Key string;
type Value string;
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
	//fmt.Println(db.dbPath)
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
	return nil;
}

func (db *DB) Get(key Key) (value Value, err error) {
	if !db.open {
		return "", errors.New(ErrNotOpen)
	}
	db.rlock(key)
	defer db.runlock(key)
	return db.read(key);
}

func (db *DB) Set(key Key, value Value) error {
	if !db.open {
		return errors.New(ErrNotOpen)
	}
	data := wrap(key, value, false)
	db.lock(key)
	defer db.unlock(key)
	return db.write(data);
}

func (db *DB) Delete(key Key) error {
	if !db.open {
		return errors.New(ErrNotOpen)
	}
	data := wrap(key, "", true)
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
		callback(err, "");
	}()
}

func (db *DB) DeleteAsync(key Key, callback Callback) {
	go func() {
		err := db.Delete(key);
		callback(err, "");
	}()
}

func (db *DB) write(packet *DataPacket) error {
	// prepared for lock and unlock of io, useless for now
	return WriteData(packet, db)
	//fmt.Println(packet)

	//TODO:
	//
	//f, err := os.OpenFile("/Users/mac/Desktop/golang/gocaskDB/abc.txt", os.O_RDWR, 0755)
	//if err != nil {
	//	fmt.Println(err)
	//}
	//b := packet.getBytes()
	//n, err := f.Write(b)
	//c := make([]byte, len(b))
	//n, err = f.Read(c)
	//fmt.Println(n, err, c)
	//dat :=  bytesToData(c)
	//fmt.Println(dat, dat.Check())
	////f.Seek()
	//f.Close()

	return nil
}

func (db *DB) read(key Key) (value Value, err error) {
	fmt.Println(key)
	return "", nil
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

//func (db DB) read(key string) (value string, err error) {
//
//}
//
//func (db DB) write() {
//	time.Now().UnixNano()
//	crc32.ChecksumIEEE()
//}