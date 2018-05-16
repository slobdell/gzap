package pool

// TODO rename this package, it doesn't make sense in terms of abtractions because there's also a pool of publishers

import (
	"sync"
	"time"
	"unsafe"

	q "github.com/slobdell/skew-binomial-queues"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	b "gzap/boundaries"
	"gzap/gpubsub"
	"gzap/inputs"
	"gzap/writers"
)

const (
	projectID = "auto-diff-android-branches"
)

var failAddress unsafe.Pointer = unsafe.Pointer(new(int8))
var topicToLoggerPool = make(map[string]*q.ThreadSafeList, 1)
var topicToLoggerLock sync.Mutex
var canonicalEncoder zapcore.Encoder

var poolFactory *gpubsub.PoolFactory

func getLoggerPoolForTopic(topicName string) *q.ThreadSafeList {
	coercePoolExistence(topicName)
	return topicToLoggerPool[topicName]
}

func coercePoolExistence(topicName string) {
	if _, ok := topicToLoggerPool[topicName]; !ok {
		topicToLoggerLock.Lock()
		defer topicToLoggerLock.Unlock()
		// check, lock, check
		if _, ok := topicToLoggerPool[topicName]; !ok {
			topicToLoggerPool[topicName] = newLoggerPool(topicName)
		}
	}
}

func newLoggerPool(topicName string) *q.ThreadSafeList {
	loggerPool := q.NewThreadSafeList()
	for i := 0; i < inputs.MaxParallelism(); i++ {
		loggerPool.InsertObject(unsafe.Pointer(newLogger(topicName)), alwaysLess)
	}
	// background flushes happen at half the period of the actually defined SLA in order
	// to satisfy Nyquist sampling
	go backgroundFlush(topicName, inputs.SLASeconds/time.Duration(2*inputs.MaxParallelism()))
	return &loggerPool
}

func WithLogger(topicName string, fn func(logger b.Logger) error) error {
	logger, cleanupFn := checkOutLogger(topicName)
	defer cleanupFn(topicName, logger)
	return fn(logger.Sugar())
}

func checkOutLogger(topicName string) (*zap.Logger, func(string, *zap.Logger)) {
	loggerPool := getLoggerPoolForTopic(topicName)
	ptr := loggerPool.PopFirst(failAddress)
	if ptr == failAddress {
		if loggerPool.LengthGreaterThan(0) {
			// simple resource contention; retry
			return checkOutLogger(topicName)
		} else {
			// all loggers are checked out, instantiate a new logger
			// this case should be an exception rather than the general case,
			// so this will balance memory pressure and network usage with a synchronous flush
			// after usage
			return newLogger(topicName), discard
		}
	}
	asLogger := (*zap.Logger)(ptr)
	return asLogger, checkInLogger
}

func checkInLogger(topicName string, logger *zap.Logger) {
	getLoggerPoolForTopic(topicName).InsertObject(unsafe.Pointer(logger), alwaysLess)
}

func discard(topicName string, logger *zap.Logger) {
	logger.Sync()
}

func newLogger(topicName string) *zap.Logger {
	publisher, err := poolFactory.CheckOut(topicName)
	if err != nil {
		// TODO better handle these errors; failing silently for now for an otherwise truly exceptional case
		return zap.NewNop()
	}
	logger, err := zap.NewProduction(
		zap.WrapCore(
			func(zapcore.Core) zapcore.Core {
				return zapcore.NewCore(
					canonicalEncoder,
					writers.NewBuffered(
						publisher,
					),
					zap.DebugLevel,
				)
			},
		),
	)
	if err != nil {
		return zap.NewNop()
	}
	return logger
}

func alwaysLess(v1 unsafe.Pointer, v2 unsafe.Pointer) bool {
	// TODO remove these functions in favor of sorting by buffer size in order to minimize
	// network calls
	return true
}

func alwaysGreater(v1 unsafe.Pointer, v2 unsafe.Pointer) bool {
	return false
}

func backgroundFlush(topicName string, sleepTime time.Duration) {
	for {
		logger, _ := checkOutLogger(topicName)
		logger.Sync()
		getLoggerPoolForTopic(
			topicName,
		).InsertObject(
			unsafe.Pointer(logger),
			alwaysGreater,
		)
		time.Sleep(sleepTime)
	}
}

func init() {
	poolFactory = gpubsub.GetPoolFactory(projectID, inputs.MaxParallelism())
	canonicalEncoder = zapcore.NewJSONEncoder(
		zap.NewProductionEncoderConfig(),
	)

}

func TearDown() {
	for topicName, loggerPool := range topicToLoggerPool {
		for loggerPool.LengthGreaterThan(0) {
			logger, _ := checkOutLogger(topicName)
			logger.Sync()
		}
	}
}
