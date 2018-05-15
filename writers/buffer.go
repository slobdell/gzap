package writers

import (
	"sync"
)

const KB = 1024
const MB = 1024 * KB
const maxMessageSize = 2 * MB

type publisher interface {
	Publish(p []byte) error
}

type Buffered struct {
	buffer      []byte
	writeCursor int
	publisher   publisher
	sync.Mutex
}

func (w *Buffered) Write(p []byte) (n int, err error) {
	if len(p) > maxMessageSize {
		if err := w.flushBuffer(); err != nil {
			return 0, err
		}
		if err := w.publisher.Publish(p); err != nil {
			return 0, err
		}
	} else if len(p)+w.writeCursor > maxMessageSize {
		if err := w.flushBuffer(); err != nil {
			return 0, err
		}
		w.appendToBuffer(p)
	} else {
		w.appendToBuffer(p)
	}
	return len(p), nil
}

func (w *Buffered) appendToBuffer(p []byte) {
	w.Lock()
	defer w.Unlock()

	for i := 0; i < len(p); i++ {
		w.buffer[w.writeCursor] = p[i]
		w.writeCursor++
	}
}

func (w *Buffered) Sync() error {
	return w.flushBuffer()
}

func (w *Buffered) flushBuffer() error {
	w.Lock()
	defer w.Unlock()
	if w.writeCursor == 0 {
		return nil
	}
	if err := w.publisher.Publish(w.buffer[0:w.writeCursor]); err != nil {
		return err
	}
	w.writeCursor = 0
	return nil
}

func NewBuffered(p publisher) *Buffered {
	return &Buffered{
		buffer:      make([]byte, maxMessageSize),
		writeCursor: 0,
		publisher:   p,
	}
}
