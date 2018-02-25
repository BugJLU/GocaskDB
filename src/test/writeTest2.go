package main

import (
	"gocaskDB"
	"fmt"
)

func main()  {
	db := &gocaskDB.DB{}
	db.Open("/Users/mac/Desktop/golang/gocaskDB/testdb/myadb.json")
	defer db.Close()
	db.Set("asdf", "eafsvgaaefraf")
	fmt.Println(db.Get("asdf"))
	db.Set("zxcv", "faeawesg")
	fmt.Println(db.Get("zxcv"))
	db.Set("asdf", "axs")
	fmt.Println(db.Get("asdf"))
	db.Delete("zxcv")
	fmt.Println(db.Get("zxcv"))
	//db.Delete("zxcv")
}