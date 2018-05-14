package gpubsub

import (
	"cloud.google.com/go/pubsub"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type gCloudGateway interface {
	MaybeCreateTopic(topicName string) error
	GetTopicPublishers(topicName string, n int) []*TopicPublisher
}

// Implements gCloudGateway
type nullGateway struct {
	originalErr error
}

func (n nullGateway) MaybeCreateTopic(topicName string) error {
	return n.originalErr
}

func (n nullGateway) GetTopicPublishers(string, int) []*TopicPublisher {
	return nil
}

func newNullGateway(originalErr error) nullGateway {
	return nullGateway{
		originalErr: originalErr,
	}
}

// Implements gCloudGateway
type gCloudClientWrapper struct {
	client    *pubsub.Client
	ctx       context.Context
	projectID string
}

func (g *gCloudClientWrapper) MaybeCreateTopic(topicName string) error {
	existingTopics, err := g.listExistingTopics()
	if err != nil {
		return err
	}
	if !listContains(existingTopics, topicName) {
		if _, err := g.createTopic(topicName); err != nil {
			return err
		}
	}
	return nil
}

func (g *gCloudClientWrapper) createTopic(topicName string) (*pubsub.Topic, error) {
	return g.client.CreateTopic(g.ctx, topicName)
}

// Publish() on GCloud Pubsub is a reasonably expensive calls with locks as well and
// is therefore an inherent bottleneck. The documentation recommends several
// topics be spawned;
func (g *gCloudClientWrapper) GetTopicPublishers(topicName string, n int) []*TopicPublisher {
	topics := make([]*TopicPublisher, n)
	for i := 0; i < n; i++ {
		topics[i] = &TopicPublisher{
			topic: g.client.Topic(topicName),
		}
	}
	return topics
}

func (g *gCloudClientWrapper) listExistingTopics() ([]string, error) {
	var topics []*pubsub.Topic
	it := g.client.Topics(g.ctx)
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
	}
	return projectMetaStripped(
		asStrings(topics),
		metaString(g.projectID),
	), nil
}

func newGCloudClientWrapper(projectID string) (*gCloudClientWrapper, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return &gCloudClientWrapper{
		ctx:       ctx,
		client:    client,
		projectID: projectID,
	}, nil
}

func newGCloudGateway(projectID string) gCloudGateway {
	wrapper, err := newGCloudClientWrapper(projectID)
	if err != nil {
		return newNullGateway(err)
	}
	return wrapper
}

func listContains(strings []string, target string) bool {
	for _, str := range strings {
		if str == target {
			return true
		}
	}
	return false
}

func metaString(projectID string) string {
	return fmt.Sprintf(
		"projects/%s/topics/",
		projectID,
	)
}

func projectMetaStripped(canonicalTopicNames []string, metaStr string) []string {
	simpleStrings := make([]string, len(canonicalTopicNames))
	for i, str := range canonicalTopicNames {
		simpleStrings[i] = str[len(metaStr):]
	}
	return simpleStrings
}

func asStrings(topics []*pubsub.Topic) []string {
	strings := make([]string, len(topics))
	for i, topic := range topics {
		strings[i] = topic.String()
	}
	return strings
}
