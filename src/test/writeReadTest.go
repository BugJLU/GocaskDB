package main

import (
	"gocaskDB"
	"util"
	"fmt"
	"strconv"
	"math/rand"
)

const AMOUNT = 100000

func main() {
	db := &gocaskDB.DB{}
	db.Open("/Users/mac/Desktop/golang/gocaskDB/testdb/testdb.json")
	defer db.Close()

	t1 := util.UnixNano()
	for i := 0; i < AMOUNT; i++ {
		istr := strconv.Itoa(i)
		db.Set(gocaskDB.Key(istr),
			gocaskDB.Value(istr+"asdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42naz"))
	}
	t2 := util.UnixNano()
	fmt.Println("Write", AMOUNT, "record(s) takes:", float32(t2-t1)/1000/1000, "ms")

	t3 := util.UnixNano()
	for i := 0; i < 10; i++ {
		key := strconv.Itoa(rand.Intn(AMOUNT))
		db.Get(gocaskDB.Key(key))
		//fmt.Print(key+" ")
		//fmt.Println(db.Get(gocaskDB.Key(key)))
	}
	t4 := util.UnixNano()
	fmt.Println("Read", AMOUNT, "record(s) takes:", float32(t4-t3)/1000/1000, "ms")
}
