package gzap

import (
	b "gzap/boundaries"
	"gzap/pool"
)

func WithLogger(topicName string, fn func(b.Logger) error) error {
	return pool.WithLogger(
		topicName,
		func(logger b.Logger) error {
			return fn(
				logger,
			)
		},
	)
}

func TearDown() {
	pool.TearDown()
}
