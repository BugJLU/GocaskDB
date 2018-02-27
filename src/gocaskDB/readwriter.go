package gocaskDB

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"util"
	"sync"
	"sort"
)

type DBinfo struct {
	Dbname string
	Serial []int32
	Active int32
}

type FileMgr struct {
	infoFile       *os.File           // Main db file including db infos.
	activeDBFile   *os.File           // Current db file to write in.
	activeHintFile *os.File           // Current hint file to write in.
	dbFiles        map[int32]*os.File // All db files.
	dbPath         string             // Directory of db.
	dbinfo         *DBinfo
	dealingNewLock *sync.Mutex
}

func (fm *FileMgr)getName(no int32) string {
	return fm.dbPath + "/" + fm.dbinfo.Dbname + "_" + strconv.Itoa(int(no))
}

// Open all relative file by name of info file.
// If db doesn't exist, a new one will be created.
func (fm *FileMgr)OpenAllFile(filename string) error {

	// open info file
	fInfo, info, err := openResolveInfoFile(filename)
	if err != nil {
		// If info file doesn't exist, create db.
		fm.dbFiles = make(map[int32]*os.File)
		if err = fm.CreateDBFiles(filename); err != nil {
			return err
		}
		return nil
	}
	fm.infoFile = fInfo
	fm.dbinfo = info

	// open active db file
	adb := fm.getName(fm.dbinfo.Active) + ".gcdb"
	fAct, err := os.OpenFile(adb, os.O_WRONLY|os.O_APPEND, 0755)
	fm.activeDBFile = fAct

	// open active hint file
	aht := fm.getName(fm.dbinfo.Active) + ".gch"
	fActHint, err := os.OpenFile(aht, os.O_WRONLY|os.O_APPEND, 0755)
	fm.activeHintFile = fActHint

	// open all db files to be read
	fm.dbFiles, err = fm.OpenAllReadDBFiles(fm.dbinfo.Serial)
	return nil
}

func openResolveInfoFile(filename string) (*os.File, *DBinfo, error) {
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
	return f, info, nil
}

// Open all db files to read from.
func (fm *FileMgr)OpenAllReadDBFiles(index []int32) (map[int32]*os.File, error) {
	fresult := make(map[int32]*os.File)
	for i := range index {
		if f, err := os.OpenFile(fm.getName(index[i])+".gcdb", os.O_RDONLY, 0755); err != nil {
			return nil, err
		} else {
			fresult[index[i]] = f
		}
	}
	return fresult, nil
}

// Open all hint files.
func (fm *FileMgr)OpenAllHintFiles(index []int32) (map[int32]*os.File, error) {
	fresult := make(map[int32]*os.File)
	for i := range index {
		if f, err := os.OpenFile(fm.getName(index[i])+".gch", os.O_RDONLY, 0755); err != nil {
			return nil, err
		} else {
			fresult[index[i]] = f
		}
	}
	return fresult, nil
}

// Called while a new db is creating.
func (fm *FileMgr)CreateDBFiles(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	info := new(DBinfo)
	info.Dbname = strings.TrimSuffix(path.Base(filename), path.Ext(filename))
	info.Active = 0 // func NewActFiles will add 1 to Active
	info.Serial = make([]int32, 0)
	fm.dbinfo = info
	fm.infoFile = f
	fm.dealingNewLock.Lock()
	if err = fm.NewActFiles(); err != nil {
		return err
	}
	if err = fm.WriteInfoFile(info); err != nil {
		return err
	}
	return nil
}

// Create or update act db files (.gcdb, .gch), update read list (dbFiles) too.
func (fm *FileMgr)NewActFiles() error {
	defer fm.dealingNewLock.Unlock()
	if fm.activeDBFile != nil {
		fm.activeDBFile.Close()
	}
	if fm.activeHintFile != nil {
		fm.activeHintFile.Close()
	}
	filename := fm.getName(fm.dbinfo.Active+1)
	// new db file
	if adb, err := os.Create(filename + ".gcdb"); err != nil {
		return err
	} else {
		fm.activeDBFile = adb
	}
	// new hint file
	if aht, err := os.Create(filename + ".gch"); err != nil {
		return err
	} else {
		fm.activeHintFile = aht
	}
	// add db file into read list
	if rdb, err := os.OpenFile(filename+".gcdb", os.O_RDONLY, 0755); err != nil {
		return err
	} else {
		fm.dbFiles[fm.dbinfo.Active+1] = rdb
	}
	// update db info
	fm.dbinfo.Active += 1
	fm.dbinfo.Serial = append(fm.dbinfo.Serial, fm.dbinfo.Active)
	fm.WriteInfoFile(fm.dbinfo)
	//fmt.Println(db.dbFiles)
	return nil
}

func (fm *FileMgr) WriteInfoFile(info *DBinfo) error {
	file := fm.infoFile
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

func (fm *FileMgr)WriteDataToFile(data *DataPacket, option DBOptions) (body *hashBody, errr error) {
	databytes := data.getBytes() //	crc	| tstmp	|  ksz	|  vsz	|  key	|  val
	var b1, b2 []byte

	// write db file
	// b1: record for .gcdb
	for i := range databytes {
		b1 = append(b1, databytes[i]...)
	}
	/* 	Lock then unlock the dealingNewLock in
	 *	case that a goroutine is running to
	 *	create new active db and hint files.
	 */
	fm.dealingNewLock.Lock()
	fm.dealingNewLock.Unlock()
	_, err := fm.activeDBFile.Write(b1)
	if err != nil {
		return nil, err
	}

	// write hint file
	// I haven't found a function in go like ftell() in c, so Size is used here instead...
	info, err := fm.activeDBFile.Stat()
	if err != nil {
		return nil, err
	}
	vpos := int32(info.Size()) - int32(binary.LittleEndian.Uint32(databytes[3])) // valpos = size(or file pointer position) - valsize
	b2 = append(b2, databytes[1]...)                                             // tstmp
	b2 = append(b2, databytes[2]...)                                             // ksz
	b2 = append(b2, databytes[3]...)                                             // vsz
	b2 = append(b2, util.ToBytes(vpos)...)                                       // vpos
	b2 = append(b2, databytes[4]...)                                             // key
	_, err = fm.activeHintFile.Write(b2)
	if err != nil {
		return nil, err
	}

	// Set hash body.
	hbody := new(hashBody)
	hbody.vpos = vpos
	hbody.vsz = data.vsz
	hbody.timestamp = data.timestamp
	hbody.fileno = fm.dbinfo.Active

	/* 	If the size of active DB file >= FILE_MAX,
	 *	turn to new active Hint and DB files.
	 */
	stat, err := os.Stat(fm.activeDBFile.Name())
	if err != nil {
		return nil, err
	}
	//fmt.Println(db.activeDBFile.Name())
	//fmt.Println(stat.Size())
	if stat.Size() >= int64(option.FILE_MAX) {
		// Lock in case that a write goroutine start before the new files are created.
		fm.dealingNewLock.Lock()
		go func() {
			err := fm.NewActFiles()
			if err != nil {
				panic(err) // halt the program
			}
		}()
	}
	return hbody, nil
}

func (fm *FileMgr)ReadValueFromFile(fileno int32, vpos int32, vsz int32) (Value, error) {
	file := fm.dbFiles[fileno]
	b := make([]byte, vsz)
	_, err := file.ReadAt(b, int64(vpos))
	if err != nil {
		return Value(0), err
	}
	return Value(b), nil
}

func (fm *FileMgr)ReadRecordFromFile(fileno int32, vpos int32, ksz int32, vsz int32) (*DataPacket, error) {
	file := fm.dbFiles[fileno]
	b := make([]byte, 20+ksz+vsz)
	_, err := file.ReadAt(b, int64(vpos-ksz-20))
	if err != nil {
		return nil, err
	}
	return bytesToData(b), err
}

func (fm *FileMgr)RebuildHashFromHint() (map[Key]*hashBody, error) {
	hashtable := make(map[Key]*hashBody)

	// open hint files
	hintfiles, err := fm.OpenAllHintFiles(fm.dbinfo.Serial)
	if err != nil {
		return nil, err
	}

	// copy serial array and sort in descending order
	sarr := make([]int, len(fm.dbinfo.Serial))
	for i := range fm.dbinfo.Serial {
		sarr[i] = int(fm.dbinfo.Serial[i])
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
			hashbody.fileno = int32(sarr[i])
			hashbody.vsz = hdata.vsz
			hashbody.vpos = hdata.vpos
			hashbody.timestamp = hdata.timestamp
			if oldbody, ok := hashtable[hdata.key]; !ok || oldbody.timestamp < hashbody.timestamp {
				//if hashbody.vsz == -1 {
				//	delete(hashtable, hdata.key)
				//} else {
				//	hashtable[hdata.key] = hashbody
				//}
				hashtable[hdata.key] = hashbody
			}
		}
	}

	return hashtable, nil
}

func readRecordFromHint(file *os.File) (*HintData, error) {
	hdata := new(HintData)
	byte8, byte4 := make([]byte, 8), make([]byte, 4)
	if _, err := file.Read(byte8); err != nil {
		return nil, err
	}
	hdata.timestamp = int64(binary.LittleEndian.Uint64(byte8))
	if _, err := file.Read(byte4); err != nil {
		return nil, err
	}
	hdata.ksz = int32(binary.LittleEndian.Uint32(byte4))
	if _, err := file.Read(byte4); err != nil {
		return nil, err
	}
	hdata.vsz = int32(binary.LittleEndian.Uint32(byte4))
	if _, err := file.Read(byte4); err != nil {
		return nil, err
	}
	hdata.vpos = int32(binary.LittleEndian.Uint32(byte4))
	byteKey := make([]byte, hdata.ksz)
	if _, err := file.Read(byteKey); err != nil {
		return nil, err
	}
	hdata.key = Key(byteKey)
	return hdata, nil
}

func (fm *FileMgr)CloseAll() error {
	if err := fm.activeDBFile.Close(); err != nil {
		return err
	}
	if err := fm.activeHintFile.Close(); err != nil {
		return err
	}
	if err := fm.infoFile.Close(); err != nil {
		return err
	}
	for i := range fm.dbFiles {
		if err := fm.dbFiles[i].Close(); err != nil {
			return err
		}
	}
	return nil
}