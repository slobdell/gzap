package gzap

import (
	b "github.com/slobdell/gzap/boundaries"
	"github.com/slobdell/gzap/pool"
)

// WithLogger takes an input topic and an input function with a boundaries.Logger object;
// Due to limitations I haven't worked around yet, Go does not play nicely passing in a function
// that takes a dependent type as a parameter.  In the caller then, the function passed must
// manually cast the logger parameter into a boundaries.Logger interface
func WithLogger(topicName string, fn func(logger interface{}) error) error {
	return pool.WithLogger(
		topicName,
		func(lg b.Logger) error {
			return fn(
				lg,
			)
		},
	)
}

func TearDown() {
	pool.TearDown()
}
