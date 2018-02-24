package main

import (
	"gocaskDB"
	"fmt"
	"util"
)

func main() {
	db := &gocaskDB.DB{}
	db.Open("/Users/mac/Desktop/golang/gocaskDB/testdb/myadb.json")

	//sem := make(chan int, 1)
	//quit := make(chan int, 100000)
	//
	//t1 := util.UnixNano()
	//for i := 0; i < 100000; i++ {
	//	sem<-1
	//	go func() {
	//		db.Set("asd123jkl0", "asdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42naz")
	//		quit<-1
	//		<-sem
	//	}()
	//}
	//for i := 0; i < 100000; i++ {
	//	<-quit
	//}
	//t2 := util.UnixNano()

	t1 := util.UnixNano()
	for i := 0; i < 100000; i++ {
		db.Set("asd123jkl0", "asdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42naz")
		//db.Delete("asd123jkl0")
	}
	t2 := util.UnixNano()

	fmt.Println(float32(t2-t1)/1000/1000, "ms")


}