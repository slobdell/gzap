package gpubsub

import (
	"sync"
)

type topicPublisherPool struct {
	topicPublishers []*TopicPublisher
	roundRobinIndex int
	sync.Mutex
}

func (t *topicPublisherPool) CheckOut() *TopicPublisher {
	t.Lock()
	defer t.Unlock()

	ret := t.topicPublishers[t.roundRobinIndex]
	t.roundRobinIndex = (t.roundRobinIndex + 1) % len(t.topicPublishers)
	return ret
}

func newTopicPublisherPool(gateway gCloudGateway, topicName string, maxParallelism int) (*topicPublisherPool, error) {
	if err := gateway.MaybeCreateTopic(topicName); err != nil {
		return nil, err
	}
	return &topicPublisherPool{
		topicPublishers: gateway.GetTopicPublishers(topicName, maxParallelism),
		roundRobinIndex: 0,
	}, nil
}
