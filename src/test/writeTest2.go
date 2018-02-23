package main

import "gocaskDB"

func main()  {
	db := &gocaskDB.DB{}
	db.Open("/Users/mac/Desktop/golang/gocaskDB/testdb/myadb.json")
	db.Set("asdf", "eafsvgaaefraf")
	db.Set("zxcv", "faeawesg")
	db.Set("asdf", "axs")
	db.Delete("zxcv")
}