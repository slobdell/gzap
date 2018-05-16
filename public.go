package gzap

import (
	"go.uber.org/zap"

	"gzap/pool"
)

type Logger interface {
	Debugw(msg string, keysAndValues ...interface{})
	Errorw(msg string, keysAndValues ...interface{})
	Infow(msg string, keysAndValues ...interface{})
}

func WithLogger(topicName string, fn func(Logger) error) error {
	return pool.WithLogger(
		topicName,
		func(logger *zap.Logger) error {
			return fn(
				logger.Sugar(),
			)
		},
	)
}
