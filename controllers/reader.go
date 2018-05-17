package controllers

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	q "github.com/slobdell/skew-binomial-queues"
	"golang.org/x/net/context"

	"github.com/slobdell/gzap/gpubsub"
	"github.com/slobdell/gzap/inputs"
)

type bufferedLine struct {
	line       string
	tsReceived time.Time
	tsServer   time.Time
}

func (b bufferedLine) Score() int64 {
	return b.tsServer.UnixNano()
}

func (b bufferedLine) RealAge() time.Duration {
	return time.Now().Sub(b.tsReceived)
}

type statefulWithChannel struct {
	serializedOutgoing chan []byte
}

func newStatefulWithChannel(serialization chan []byte) *statefulWithChannel {
	return &statefulWithChannel{
		serializedOutgoing: serialization,
	}
}

func (s *statefulWithChannel) dequeueMessages(ctx context.Context, data []byte) {
	s.serializedOutgoing <- data
}

func unmarshalWorker(serializedIncoming chan []byte, preparedOutgoing chan q.PriorityQ) {
	for {
		asPriorityQ := q.NewImmutableSynchronousQ()
		data := <-serializedIncoming
		lines := strings.Split(string(data), "\n")
		// the entire message will be terminated by a newline, so the last line is always empty
		lines = lines[:len(lines)-1]

		for _, line := range lines {
			tsServer, err := extractTimeFromLine(line)
			if err != nil {
				// TODO(slobdell) should this case be more adequately handlded?
				continue
			}
			asPriorityQ = asPriorityQ.Enqueue(
				&bufferedLine{
					line:       line,
					tsReceived: time.Now(),
					tsServer:   tsServer,
				},
			)
		}
		preparedOutgoing <- asPriorityQ
	}
}

func orderedOutputWorker(orderedQs chan q.PriorityQ) {
	finalOrderedQ := q.NewImmutableSynchronousQ()
	tick := time.Tick(50 * time.Millisecond)
	for {
		select {
		case <-tick:
			for {
				if finalOrderedQ.IsEmpty() {
					break
				}
				bLine, _ := finalOrderedQ.Peek().(*bufferedLine)
				if bLine.RealAge() < inputs.SLASeconds {
					break
				}
				var highestPriority q.PriorityScorer
				highestPriority, finalOrderedQ = finalOrderedQ.Dequeue()
				bLine, _ = highestPriority.(*bufferedLine)
				fmt.Println(string(bLine.line))
			}
		case unmergedQ := <-orderedQs:
			finalOrderedQ = finalOrderedQ.Meld(unmergedQ)
		}
	}
}

func extractTimeFromLine(line string) (time.Time, error) {
	genericJSON := make(map[string]interface{})
	if err := json.Unmarshal([]byte(line), &genericJSON); err != nil {
		return time.Time{}, err
	}
	return epochToTime(genericJSON["ts"].(float64)), nil
}

func epochToTime(epoch float64) time.Time {
	seconds := int64(epoch)
	remainingFloat := epoch - float64(seconds)
	nanos := int64(remainingFloat * float64(time.Second))
	return time.Unix(seconds, nanos)
}

func PrintIncoming(topicName string) (func(), error) {
	serialization := make(chan []byte)
	gateway := gpubsub.NewGCloudGateway(
		inputs.GoogleProjectID(),
	)
	if err := gateway.MaybeCreateTopic(topicName); err != nil {
		return func() {}, err
	}
	recvFn, deferFn, err := gateway.SubscribeNowWithHandler(
		topicName,
		newStatefulWithChannel(serialization).dequeueMessages,
	)
	if err != nil {
		return func() {}, err
	}
	go recvFn()

	orderedQs := make(chan q.PriorityQ)
	for i := 0; i < inputs.MaxParallelism()-1; i++ {
		go unmarshalWorker(serialization, orderedQs)
	}
	go orderedOutputWorker(orderedQs)
	return deferFn, err
}
