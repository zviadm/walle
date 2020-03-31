package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	storageFlushQ   = 8192
	streamPipelineQ = 256 // TODO(zviad): This should be in MBs.
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
	flushQ              chan *ResultCtx
	fetchCommittedEntry fetchFunc

	mx sync.Mutex
	p  map[string]*streamPipeline
}

func New(
	ctx context.Context,
	flushSync func(),
	fetchCommittedEntry fetchFunc) *Pipeline {
	r := &Pipeline{
		rootCtx:             ctx,
		flushSync:           flushSync,
		flushQ:              make(chan *ResultCtx, storageFlushQ),
		fetchCommittedEntry: fetchCommittedEntry,
		p:                   make(map[string]*streamPipeline),
	}
	go r.flusher(ctx)
	return r
}

func (s *Pipeline) ForStream(ss storage.Stream) *streamPipeline {
	s.mx.Lock()
	defer s.mx.Unlock()
	p, ok := s.p[ss.StreamURI()]
	if !ok {
		p = newStreamPipeline(s.rootCtx, ss, s.flushQ, s.fetchCommittedEntry)
		s.p[ss.StreamURI()] = p
	}
	return p
}

func (s *Pipeline) QueueFlush() *ResultCtx {
	r := newResult()
	s.flushQ <- r
	return r
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

type streamPipeline struct {
	ss                  storage.Stream
	flushQ              chan<- *ResultCtx
	fetchCommittedEntry fetchFunc

	q *queue
}

func newStreamPipeline(
	ctx context.Context,
	ss storage.Stream,
	flushQ chan<- *ResultCtx,
	fetchCommittedEntry fetchFunc) *streamPipeline {
	r := &streamPipeline{
		ss:                  ss,
		flushQ:              flushQ,
		fetchCommittedEntry: fetchCommittedEntry,
		q:                   newQueue(),
	}
	go r.Process(ctx)
	return r
}

func (p *streamPipeline) waitForReady(
	ctx context.Context, maxId int64) (int64, error) {
	for {
		if p.q.Len() > streamPipelineQ {
			zlog.Warningf(
				"[sp] pipeline queue is overflowing %s: %d, maxId: %d",
				p.ss.StreamURI(), p.q.Len(), maxId)
			return maxId, nil
		}

		head, queueNotify := p.q.Peek()
		if head == nil {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-queueNotify:
			}
			continue
		}
		if head.IsReady(maxId) {
			return maxId, nil
		}
		tailId, tailNotify := p.ss.TailEntryId()
		maxId = tailId
		if head.IsReady(tailId) {
			return maxId, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-tailNotify:
		case <-queueNotify:
		case <-time.After(wallelib.LeaseMinimum / 4):
			if p.q.PopTillCommitted() {
				return maxId, nil
			}
		}
	}
}

func (p *streamPipeline) backfillEntry(
	ctx context.Context, entryId int64, entryMd5 []byte) bool {
	_, _, writerLease, _ := p.ss.WriterInfo()
	if writerLease < wallelib.LeaseMinimum {
		writerLease = wallelib.LeaseMinimum
	}
	ctx, cancel := context.WithTimeout(ctx, writerLease/4)
	defer cancel()
	entry, err := p.fetchCommittedEntry(
		ctx, p.ss.StreamURI(), entryId, entryMd5)
	if err != nil {
		zlog.Warningf("[sp] err fetching: %s:%d - %s", p.ss.StreamURI(), entryId, err)
		return false
	}
	ok := p.ss.PutEntry(entry, true)
	panic.OnNotOk(ok, "committed putEntry must always succeed")
	zlog.Infof("[sp] stream: %s caught up to: %d (might have created a gap)", p.ss.StreamURI(), entryId)
	return true
}

func (p *streamPipeline) Process(ctx context.Context) {
	var maxId int64
	for ctx.Err() == nil {
		var err error
		maxId, err = p.waitForReady(ctx, maxId)
		if err != nil {
			return
		}
		req := p.q.Pop()
		var ok bool
		if req.R.Entry == nil {
			ok = p.ss.CommitEntry(req.R.EntryId, req.R.EntryMd5)
			if !ok {
				ok = p.backfillEntry(ctx, req.R.EntryId, req.R.EntryMd5)
			}
		} else {
			ok = p.ss.PutEntry(req.R.Entry, req.R.Committed)
		}
		if ok && req.R.EntryId > maxId {
			maxId = req.R.EntryId
		}
		if !ok {
			req.Res.set(status.Errorf(codes.OutOfRange, "entryId: %d", req.R.EntryId))
		} else {
			p.flushQ <- req.Res
		}
	}
}

func (p *streamPipeline) QueueCommit(entryId int64, entryMd5 []byte) *ResultCtx {
	return p.q.Queue(&Request{
		EntryId:   entryId,
		EntryMd5:  entryMd5,
		Committed: true,
	})
}
func (p *streamPipeline) QueuePut(e *walleapi.Entry, isCommitted bool) *ResultCtx {
	return p.q.Queue(&Request{
		EntryId:   e.EntryId,
		EntryMd5:  e.ChecksumMd5,
		Committed: isCommitted,
		Entry:     e,
	})
}
