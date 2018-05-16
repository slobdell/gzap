package writers_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"gzap/writers"
)

type testPublisher struct {
	publishedData []byte
}

func (t *testPublisher) Publish(p []byte) error {
	t.publishedData = make([]byte, len(p))
	for i := 0; i < len(p); i++ {
		t.publishedData[i] = p[i]
	}
	return nil
}

func TestBasicWrite(t *testing.T) {
	publisher := &testPublisher{}
	buff := writers.NewBuffered(publisher)
	msg := []byte("Hello world")
	buff.Write(msg)

	assert.Equal(t, 0, len(publisher.publishedData), "Nothing should be published yet")
	buff.Sync()
	assert.Equal(t, []byte("Hello world"), publisher.publishedData, "Buffer should now be flushed")
}

func TestNewMessageExceedsBuffer(t *testing.T) {
}
