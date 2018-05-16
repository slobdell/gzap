package gpubsub

import (
	"fmt"
	"sync"
)

var longLivedPoolFactories = make(map[string]*PoolFactory, 1)
var poolFactoryLock sync.Mutex

type PoolFactory struct {
	topicToPublishers map[string]*topicPublisherPool
	projectID         string
	maxParallelism    int
	sync.Mutex
}

func (p *PoolFactory) CheckOut(topicName string) (*TopicPublisher, error) {
	if err := p.coerceTopicPool(topicName); err != nil {
		return nil, err
	}
	return p.topicToPublishers[topicName].CheckOut(), nil
}

func (p *PoolFactory) coerceTopicPool(topicName string) error {
	if !p.topicInitialized(topicName) {
		p.Lock()
		defer p.Unlock()
		// check, lock, check; it's possible for resource contention here
		if !p.topicInitialized(topicName) {
			pool, err := newTopicPublisherPool(
				NewGCloudGateway(p.projectID),
				topicName,
				p.maxParallelism,
			)
			if err != nil {
				return err
			}
			p.topicToPublishers[topicName] = pool
		}
	}
	return nil
}

func (p *PoolFactory) topicInitialized(topicName string) bool {
	_, ok := p.topicToPublishers[topicName]
	return ok
}

func GetPoolFactory(projectID string, maxParallelism int) *PoolFactory {
	key := factoryKey(projectID, maxParallelism)
	_, ok := longLivedPoolFactories[key]
	if !ok {
		poolFactoryLock.Lock()
		defer poolFactoryLock.Unlock()
		// check, lock, check
		if _, ok := longLivedPoolFactories[key]; !ok {
			longLivedPoolFactories[key] = &PoolFactory{
				topicToPublishers: make(map[string]*topicPublisherPool, 1),
				projectID:         projectID,
				maxParallelism:    maxParallelism,
			}
		}
	}
	return longLivedPoolFactories[key]
}

func factoryKey(projectID string, maxParallelism int) string {
	return fmt.Sprintf("%s:%d", projectID, maxParallelism)
}
