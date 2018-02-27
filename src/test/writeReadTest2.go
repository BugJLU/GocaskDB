package main

import (
	"fmt"
	"gocaskDB"
	"math/rand"
	"strconv"
)

func main() {
	db := &gocaskDB.DB{}
	//db.Open("/Users/mac/Desktop/golang/gocaskDB/testdb/test1db.json")
	//defer db.Close()

	changes := make(map[int]bool)

	db.Open("/Users/mac/Desktop/golang/gocaskDB/testdb/test1db.json")
	for i := 0; i < 50; i++ {
		istr := strconv.Itoa(i)
		db.Set(gocaskDB.Key(istr),
			gocaskDB.Value(istr+"asdfgzxcvnuedjszqiklwerxsqi175'[z056=`z/eklzr42naz"))
	}
	db.Close()

	db.Open("/Users/mac/Desktop/golang/gocaskDB/testdb/test1db.json")
	for i := 0; i < 10; i++ {
		ikey := rand.Intn(50) // / 2
		key := strconv.Itoa(ikey)
		db.Delete(gocaskDB.Key(key))
		changes[ikey] = true
	}
	db.Close()

	db.Open("/Users/mac/Desktop/golang/gocaskDB/testdb/test1db.json")
	for i := 0; i < 10; i++ {
		ikey := rand.Intn(50) // / 2 + 25
		key := strconv.Itoa(ikey)
		db.Set(gocaskDB.Key(key), gocaskDB.Value(key))
		changes[ikey] = true
	}
	db.Close()

	db.Open("/Users/mac/Desktop/golang/gocaskDB/testdb/test1db.json")
	for i := 0; i < 50; i++ {
		if _, ok := changes[i]; ok {
			fmt.Print("* ")
		}
		fmt.Println(db.Get(gocaskDB.Key(strconv.Itoa(i))))
	}
	db.Close()

}
