package main

import (
	"fmt"
	"gocaskDB"
	"math/rand"
	"strconv"
	"util"
)

func main() {
	db := &gocaskDB.DB{}
	option := gocaskDB.GetDefaultOption()
	option.FILE_MAX = 128 << 20 // 128MB
	//option.READ_CHECK = true
	db.OpenWithOptions("/Users/mac/Desktop/golang/gocaskDB/testdb/testdb.json", option)
	defer db.Close()

	//fmt.Println(db.Get("123"))
	t1 := util.UnixNano()
	for i := 0; i < 500000; i++ {
		db.Get(gocaskDB.Key(strconv.Itoa(rand.Intn(500000))))
	}
	t2 := util.UnixNano()
	fmt.Println(float32(t2-t1) / 1000 / 1000)
}
