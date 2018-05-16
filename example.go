package main

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	"gzap/pool"
)

func example() {
	defer pool.TearDown()
	err := pool.WithLogger(
		"example.topic",
		func(logger *zap.Logger) error {
			logger.Sugar().Infow(
				"Doing something really important",
				"url", "https://google.com",
				"some_num", 5,
				"i", i,
			)
			// do stuff
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Sleeping in order to allow background buffers to send\n")
	time.Sleep(5 * time.Second)
}
