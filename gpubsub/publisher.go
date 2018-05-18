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
			Data: copied(p),
		},
	)
	// SBL dont know if we need this yet
	publishResult = publishResult
	return nil
}

func copied(p []byte) []byte {
	c := make([]byte, len(p))
	for i, b := range p {
		c[i] = b
	}
	return c
}
