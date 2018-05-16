package gpubsub

import (
	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

type TopicPublisher struct {
	topic *pubsub.Topic
}

func (t *TopicPublisher) Publish(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	ctx := context.Background()
	publishResult := t.topic.Publish(
		ctx,
		&pubsub.Message{
			Data: p,
		},
	)
	// SBL dont know if we need this yet
	publishResult = publishResult
	return nil
}
