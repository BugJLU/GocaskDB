package gocaskDB

import (
	//"os"
	//"io"
	//"bufio"
	"os"
	"io/ioutil"
	"encoding/json"
	//"fmt"
	//"path/filepath"
	"io"
	//"fmt"
	"path"
	"strings"
	"encoding/binary"
	"util"
	"strconv"
)

type DBinfo struct {
	Dbname string
	Serial []int32
	Active int32
}

func getName(no int32, db *DB) string {
	return db.dbPath+"/"+db.dbinfo.Dbname+"_"+strconv.Itoa(int(no))
}

// Open all relative file by name of info file.
// If db doesn't exist, a new one will be created.
func OpenAllFile(filename string, db *DB) error {

	// open info file
	fInfo, info, err := OpenResolveInfoFile(filename)
	if err != nil {
		// If info file doesn't exist, create db.
		if err = CreateDBFiles(filename, db); err != nil {
			return err
		}
		return nil
	}
	db.infoFile = fInfo
	db.dbinfo = info

	// open active db file
	adb := getName(db.dbinfo.Active, db)+".gcdb"
	fAct, err := os.OpenFile(adb, os.O_WRONLY|os.O_APPEND, 0755)
	db.activeDBFile = fAct

	// open active hint file
	aht := getName(db.dbinfo.Active, db)+".gch"
	fActHint, err := os.OpenFile(aht, os.O_WRONLY|os.O_APPEND, 0755)
	db.activeHintFile = fActHint

	// open all db files to be read
	rfiles := make([]string, len(db.dbinfo.Serial))
	for i := range db.dbinfo.Serial{
		rfiles = append(rfiles, getName(db.dbinfo.Serial[i], db)+".gcdb")
	}
	fRead, err := OpenAllReadDBFiles(rfiles)
	db.dbFiles = fRead;

	return nil
}

func CreateDBFiles(filename string, db *DB) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	info := new(DBinfo)
	info.Dbname = strings.TrimSuffix(path.Base(filename), path.Ext(filename))
	info.Active = 0;	// func NewActFiles will add 1 to Active
	info.Serial = make([]int32, 0)
	db.dbinfo = info
	db.infoFile = f
	db.dealingLock.Lock()
	if err = NewActFiles(db); err != nil {
		return err
	}
	if err = WriteInfoFile(f, info); err != nil {
		return err
	}
	return nil
}

func OpenResolveInfoFile(filename string) (*os.File, *DBinfo, error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0755)
	if err != nil {
		return nil, nil, err
	}
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, nil, err
	}
	info := new(DBinfo)
	err = json.Unmarshal(bytes, info)
	if err != nil {
		return nil, nil, err
	}
	return f, info, nil;
}

func WriteInfoFile(file *os.File, info *DBinfo) error {
	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	err = file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = file.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

// Open db files to read from.
func OpenAllReadDBFiles(filename []string) ([]*os.File, error)  {
	return nil, nil
}

func OpenReadDBFile(filename string) (*os.File, error) {
	return nil, nil
}

func WriteData(data *DataPacket, db *DB) (body *hashBody, errr error) {
	databytes := data.getBytes()	//	crc	| tstmp	|  ksz	|  vsz	|  key	|  val
	var b1, b2 []byte

	// write db file
	// b1: record for .gcdb
	for i := range databytes {
		b1 = append(b1, databytes[i]...)
	}
	_, err := db.activeDBFile.Write(b1)
	if err != nil {
		return nil, err
	}

	// write hint file
	// I haven't found a function in go like ftell() in c, so Size is used here instead...
	info, err := db.activeDBFile.Stat();
	if err != nil {
		return nil, err
	}
	vpos := int32(info.Size()) - int32(binary.LittleEndian.Uint32(databytes[3])) // valpos = size(or file pointer position) - valsize
	b2 = append(b2, databytes[1]...)	// tstmp
	b2 = append(b2, databytes[2]...)	// ksz
	b2 = append(b2, databytes[3]...)	// vsz
	b2 = append(b2, util.ToBytes(vpos)...)	// vpos
	b2 = append(b2, databytes[4]...)	// key
	_, err = db.activeHintFile.Write(b2)
	if err != nil {
		return nil, err
	}

	// Set hash body.
	hbody := new(hashBody)
	hbody.vpos = vpos
	hbody.vsz = data.vsz
	hbody.timestamp = data.timestamp
	//hbody.file =


	/* 	If the size of active DB file >= file_max,
	 *	turn to new active Hint and DB files.
	*/
	stat, err := os.Stat(db.activeDBFile.Name())
	if err != nil {
		return nil, err
	}
		//fmt.Println(db.activeDBFile.Name())
		//fmt.Println(stat.Size())
	if stat.Size() >= int64(db.options.file_max) {
		// Lock in case that a write goroutine start before the new files are created.
		db.dealingLock.Lock()
		go func() {
			err := NewActFiles(db)
			if err != nil {
				panic(err) // halt the program
			}
		}()
	}
	return hbody, nil
}

func NewActFiles(db *DB) error {
	defer db.dealingLock.Unlock()
	if db.activeDBFile != nil {
		db.activeDBFile.Close()
	}
	if db.activeHintFile != nil {
		db.activeHintFile.Close()
	}
	filename := getName(db.dbinfo.Active + 1, db)
	if adb, err := os.Create(filename+".gcdb"); err != nil {
		return err
	} else {
		db.activeDBFile = adb
	}
	if aht, err := os.Create(filename+".gch"); err != nil {
		return err
	} else {
		db.activeHintFile = aht
	}
	// update db info
	db.dbinfo.Active += 1
	db.dbinfo.Serial = append(db.dbinfo.Serial, db.dbinfo.Active)
	WriteInfoFile(db.infoFile, db.dbinfo)
	return nil
}

/* Useless */
//// Open hint files to read from.
//func OpenReadHintFiles(filename []string) ([]*os.File, error)  {
//	return nil, nil
//}


