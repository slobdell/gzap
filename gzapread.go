package main

import (
	"os"

	"gzap/controllers"
)

func main() {
	if len(os.Args) == 1 {
		panic("Usage: gzap example.topc")
	}
	topicName := os.Args[1]
	deferFn, err := controllers.PrintIncoming(topicName)
	if err != nil {
		panic(err)
	}
	defer deferFn()
	select {}
}
