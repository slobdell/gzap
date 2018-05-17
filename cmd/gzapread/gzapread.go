package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/slobdell/gzap/controllers"
)

var sigChan = make(chan os.Signal)

func main() {
	if len(os.Args) == 1 {
		fmt.Printf("Usage: gzap example.topc\n")
		os.Exit(1)
	}

	topicName := os.Args[1]
	deferFn, err := controllers.PrintIncoming(topicName)
	if err != nil {
		panic(err)
	}
	go cleanupOnInterrupt(deferFn)
	select {}
}

func cleanupOnInterrupt(cleanupFn func()) {
	<-sigChan
	cleanupFn()
	os.Exit(0)
}

func init() {
	signal.Notify(
		sigChan,
		os.Interrupt,
		os.Kill,
	)
}
