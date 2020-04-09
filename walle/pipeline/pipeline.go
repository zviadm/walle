package pipeline

import (
	"context"
	"sync"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
)

const (
	storageFlushQ = 8192
)

type fetchFunc func(
	ctx context.Context,
	streamURI string,
	committedId int64,
	committedMd5 []byte) (*walleapi.Entry, error)

// Pipeline provides queue like abstraction to stream line
// put operations for each stream, and perform group FlushSync operations
// for much better overall throughput.
type Pipeline struct {
	rootCtx             context.Context
	maxStreamQueueSize  int
	flushSync           func()
	flushQ              chan *ResultCtx
	fetchCommittedEntry fetchFunc

	mx sync.Mutex
	p  map[storage.Stream]*stream
}

// New creates new Pipeline object.
func New(
	ctx context.Context,
	maxStreamQueueSize int,
	flushSync func(),
	fetchCommittedEntry fetchFunc) *Pipeline {
	r := &Pipeline{
		rootCtx:             ctx,
		maxStreamQueueSize:  maxStreamQueueSize,
		flushSync:           flushSync,
		flushQ:              make(chan *ResultCtx, storageFlushQ),
		fetchCommittedEntry: fetchCommittedEntry,
		p:                   make(map[storage.Stream]*stream),
	}
	go r.flusher(ctx)
	return r
}

// ForStream returns pipeline queue for a specific stream.
func (s *Pipeline) ForStream(ss storage.Stream) *stream {
	s.mx.Lock()
	defer s.mx.Unlock()
	p, ok := s.p[ss]
	if !ok {
		p = newStream(s.rootCtx, ss, s.maxStreamQueueSize, s.fetchCommittedEntry)
		s.p[ss] = p
	}
	return p
}

// Flush schedules and waits for next flushSync operation to succeed.
func (s *Pipeline) Flush() {
	r := newResult()
	s.flushQ <- r
	<-r.Done()
	return
}

func (s *Pipeline) flusher(ctx context.Context) {
	q := make([]*ResultCtx, 0, storageFlushQ)
	for {
		q = q[:0]
		select {
		case <-ctx.Done():
			return
		case r := <-s.flushQ:
			q = append(q, r)
		}
	DrainLoop:
		for {
			select {
			case r := <-s.flushQ:
				q = append(q, r)
			default:
				break DrainLoop
			}
		}
		s.flushSync()
		for _, r := range q {
			r.set(nil)
		}
	}
}
