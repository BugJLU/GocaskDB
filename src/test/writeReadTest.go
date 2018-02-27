package main

import (
	"fmt"
	"gocaskDB"
	"math/rand"
	"strconv"
	"util"
)

const AMOUNT = 500000

func main() {
	db := &gocaskDB.DB{}
	option := gocaskDB.GetDefaultOption()
	option.FILE_MAX = 128 << 20 // 128MB
	//option.READ_CHECK = true
	db.OpenWithOptions("/Users/mac/Desktop/golang/gocaskDB/testdb/testdb.json", option)
	defer db.Close()

	//t1 := util.UnixNano()
	//for i := 0; i < AMOUNT; i++ {
	//	istr := strconv.Itoa(i)
	//	db.Set(gocaskDB.Key(istr),
	//		gocaskDB.Value(istr+"asdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42nazasdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42nazasdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42nazasdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42nazasdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42nazasdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42nazasdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42nazasdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42nazasdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42nazasdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42naz"))
	//}
	//t2 := util.UnixNano()
	//fmt.Println("Write", AMOUNT, "record(s) takes:", float32(t2-t1)/1000/1000, "ms")

	sli := make([]string, 0)
	for i := 0; i < AMOUNT; i++ {
		key := strconv.Itoa(rand.Intn(AMOUNT))
		sli = append(sli, key)
	}
	t3 := util.UnixNano()
	for i := 0; i < AMOUNT; i++ {
		//key := sli[i]
		db.Get(gocaskDB.Key(sli[i]))
		//fmt.Print(key+" ")
		//fmt.Println(db.Get(gocaskDB.Key(key)))
	}
	t4 := util.UnixNano()
	fmt.Println("Read", AMOUNT, "record(s) takes:", float32(t4-t3)/1000/1000, "ms")
}
