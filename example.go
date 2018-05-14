package main

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	"gzap/pool"
)

func TearDown() {
	pool.TearDown()
}

func main() {
	defer TearDown()
	err := pool.WithLogger(
		"example.topic",
		func(logger *zap.SugaredLogger) error {
			logger.Infow(
				"Doing something really important",
				"url", "https://google.com",
				"some_num", 5,
			)
			// do stuff
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("sleeping for a bit\n")
	time.Sleep(5 * time.Second)
}
