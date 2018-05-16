# GZap

GZap is a full-fledged logging library written for a use case of tailing server-side structured logs in real-time.  It provides both an API for logging from an application and the corresponding tool to read those logs client-side.  It makes use of Uber's Open Source [Zap Library](https://github.com/uber-go/zap) for high performance logging outside of the hot path and Google Cloud's [Pub/Sub](https://cloud.google.com/pubsub/)

# Getting Started

Currently, GZap requires that the `GOOGLE_PROJECT_ID` environment variable is set to your canonical Google Cloud Project ID. Pub/Sub topics and subscriptions will be managed behind the scenes. In your application, you can log to a topic with:

```
import (
        "go.uber.org/zap"
        "gzap/pool"
)

var arbitraryTopicName = "example.topic"
var arbitraryURL = "https://google.com"
var arbitraryInt = 5

func example() error {
  return pool.WithLogger(
    arbitraryTopicName,
    func(logger *zap.Logger) error {
      logger.Sugar().Infow(
        "I'm about to do some work!",
        "url", arbitraryURL,
        "some_num", arbitraryInt,
      )
      
      // do work
      
      return nil
    },
  )
}
```

And from a client terminal, that log can be read with:

`$GOPATH/bin/gzap example.topic`

# Motivation

The primary benefit to this library and by extension the nature of the provided API is that it presupposes that *logging will occur over the network*.  Therefore, GZap will take steps to buffer appropriately, minimize network calls, never block on the hot path, take advantage of multi-core machines, and establish a ceiling for memory and processor usage.
