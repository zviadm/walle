package pipeline

import (
	"context"
	"sync"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"go.uber.org/atomic"
)

type fetchFunc func(
	ctx context.Context,
	streamURI string,
	committedId int64,
	committedXX uint64) (*walleapi.Entry, error)

type notifyGapFunc func(streamURI string)

// Pipeline provides queue like abstraction to stream line
// put operations for each stream, and perform group FlushSync operations
// for much better overall throughput.
type Pipeline struct {
	rootCtx             context.Context
	flushSync           func()
	fetchCommittedEntry fetchFunc
	notifyGap           notifyGapFunc

	mx     sync.Mutex
	p      map[storage.Stream]*stream
	totalQ atomic.Int64
}

// New creates new Pipeline object.
func New(
	ctx context.Context,
	fetchCommittedEntry fetchFunc,
	notifyGap notifyGapFunc) *Pipeline {
	r := &Pipeline{
		rootCtx:             ctx,
		fetchCommittedEntry: fetchCommittedEntry,
		notifyGap:           notifyGap,
		p:                   make(map[storage.Stream]*stream),
	}
	return r
}

// ForStream returns pipeline queue for a specific stream.
func (s *Pipeline) ForStream(ss storage.Stream) *stream {
	s.mx.Lock()
	defer s.mx.Unlock()
	p, ok := s.p[ss]
	if !ok {
		p = newStream(s.rootCtx, ss, s.fetchCommittedEntry, s.notifyGap, &s.totalQ)
		s.p[ss] = p
	}
	return p
}
