package gzap

import (
	"github.com/slobdell/gzap/pool"
)

type Logger interface {
	Debugw(msg string, keysAndValues ...interface{})
	Errorw(msg string, keysAndValues ...interface{})
	Infow(msg string, keysAndValues ...interface{})
}

func CheckOutLogger(topicName string) (Logger, func()) {
	zLogger, deferFn := pool.CheckOutLogger(topicName)
	return zLogger.Sugar(), func() {
		deferFn(zLogger)
	}
}

func TearDown() {
	pool.TearDown()
}
