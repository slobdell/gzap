package pool

// TODO rename this package, it doesn't make sense in terms of abtractions because there's also a pool of publishers

import (
	"runtime"
	"sync"
	"time"
	"unsafe"

	q "github.com/slobdell/skew-binomial-queues"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"gzap/gpubsub"
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

const milliSLA = 1000

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
	for i := 0; i < maxParallelism(); i++ {
		loggerPool.InsertObject(unsafe.Pointer(newLogger(topicName)), alwaysLess)
	}
	go backgroundFlush(topicName, milliSLA/maxParallelism())
	return &loggerPool
}

func WithLogger(topicName string, fn func(logger *zap.SugaredLogger) error) error {
	logger, cleanupFn := checkOutLogger(topicName)
	defer cleanupFn(topicName, logger)
	return fn(logger)
}

func checkOutLogger(topicName string) (*zap.SugaredLogger, func(string, *zap.SugaredLogger)) {
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
	asLogger := (*zap.SugaredLogger)(ptr)
	return asLogger, checkInLogger
}

func checkInLogger(topicName string, logger *zap.SugaredLogger) {
	getLoggerPoolForTopic(topicName).InsertObject(unsafe.Pointer(logger), alwaysLess)
}

func discard(topicName string, logger *zap.SugaredLogger) {
	logger.Sync()
}

func maxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}

func newLogger(topicName string) *zap.SugaredLogger {
	publisher, err := poolFactory.CheckOut(topicName)
	if err != nil {
		// TODO better handle these errors; failing silently for now for an otherwise truly exceptional case
		return zap.NewNop().Sugar()
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
		return zap.NewNop().Sugar()
	}
	return logger.Sugar()
}

func alwaysLess(v1 unsafe.Pointer, v2 unsafe.Pointer) bool {
	// TODO remove these functions in favor of sorting by buffer size in order to minimize
	// network calls
	return true
}

func alwaysGreater(v1 unsafe.Pointer, v2 unsafe.Pointer) bool {
	return false
}

func backgroundFlush(topicName string, sleepMillis int) {
	for {
		logger, _ := checkOutLogger(topicName)
		logger.Sync()
		getLoggerPoolForTopic(
			topicName,
		).InsertObject(
			unsafe.Pointer(logger),
			alwaysGreater,
		)
		time.Sleep(time.Duration(sleepMillis) * time.Millisecond)
	}
}

func init() {
	poolFactory = gpubsub.GetPoolFactory(projectID, maxParallelism())
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
