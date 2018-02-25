package main

import "gocaskDB"

func main()  {
	db := &gocaskDB.DB{}
	db.Open("/Users/mac/Desktop/golang/gocaskDB/testdb/myadb.json")
	defer db.Close()
	db.Set("asdf", "eafsvgaaefraf")
	db.Set("zxcv", "faeawesg")
	db.Set("asdf", "axs")
	//db.Delete("zxcv")
}