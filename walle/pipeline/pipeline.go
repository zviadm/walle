package pipeline

import (
	"context"
	"sync"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
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
	flushSync           func()
	fetchCommittedEntry fetchFunc

	mx sync.Mutex
	p  map[storage.Stream]*stream

	flushMX   sync.Mutex
	flushQ    chan struct{}
	flushDone chan struct{}
}

// New creates new Pipeline object.
func New(
	ctx context.Context,
	flushSync func(),
	fetchCommittedEntry fetchFunc) *Pipeline {
	r := &Pipeline{
		rootCtx:             ctx,
		flushSync:           flushSync,
		fetchCommittedEntry: fetchCommittedEntry,
		p:                   make(map[storage.Stream]*stream),

		flushQ:    make(chan struct{}, 1),
		flushDone: make(chan struct{}),
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
		p = newStream(s.rootCtx, ss, s.fetchCommittedEntry)
		s.p[ss] = p
	}
	return p
}

// Flush schedules and waits for next flushSync operation to succeed.
func (s *Pipeline) Flush(ctx context.Context) error {
	s.flushMX.Lock()
	done := s.flushDone
	s.flushMX.Unlock()
	select {
	case s.flushQ <- struct{}{}:
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	return nil
}

func (s *Pipeline) flusher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.flushQ:
		}
		s.flushMX.Lock()
		flushDone := s.flushDone
		s.flushDone = make(chan struct{})
		s.flushMX.Unlock()

		s.flushSync()
		close(flushDone)
	}
}
