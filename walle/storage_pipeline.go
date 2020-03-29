package walle

import (
	"container/heap"
	"context"
	"sync"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
)

const (
	storageFlushQ = 8192
)

// storagePipeline provides queue like abstraction to stream line
// put operations for each stream, and perform group FlushSync operations
// for much better overall throughput.
type storagePipeline struct {
	rootCtx   context.Context
	flushSync func()
	flushQ    chan chan bool

	mx sync.Mutex
	p  map[string]*streamPipeline
}

func newStoragePipeline(ctx context.Context, flushSync func()) *storagePipeline {
	r := &storagePipeline{
		rootCtx:   ctx,
		flushSync: flushSync,
		flushQ:    make(chan chan bool, storageFlushQ),
		p:         make(map[string]*streamPipeline),
	}
	go r.flusher(ctx)
	return r
}

func (s *storagePipeline) ForStream(ss StreamStorage) *streamPipeline {
	s.mx.Lock()
	defer s.mx.Unlock()
	p, ok := s.p[ss.StreamURI()]
	if !ok {
		p = newStreamPipeline(s.rootCtx, ss, s.flushQ)
		s.p[ss.StreamURI()] = p
	}
	return p
}

func (s *storagePipeline) WaitForFlush() {
	c := make(chan bool, 1)
	s.flushQ <- c
	<-c
}

func (s *storagePipeline) flusher(ctx context.Context) {
	q := make([]chan bool, 0, storageFlushQ)
	for {
		q = q[:0]
		select {
		case <-ctx.Done():
			return
		case c := <-s.flushQ:
			q = append(q, c)
		}
	DrainLoop:
		for {
			select {
			case c := <-s.flushQ:
				q = append(q, c)
			default:
				break DrainLoop
			}
		}
		s.flushSync()
		for _, c := range q {
			c <- true
		}
	}
}

type streamPipeline struct {
	ss     StreamStorage
	flushQ chan<- chan bool

	mx      sync.Mutex
	q       *pipelineQueue
	qNotify chan struct{}
}

type pipelineReq struct {
	R   *walle_pb.PutEntryInternalRequest
	okC chan bool
}

type pipelineQueue struct {
	v []*pipelineReq
}

func (q *pipelineQueue) Less(i, j int) bool {
	return waitIdForRequest(q.v[i].R) < waitIdForRequest(q.v[j].R)
}
func (q *pipelineQueue) Len() int      { return len(q.v) }
func (q *pipelineQueue) Swap(i, j int) { (q.v)[i], (q.v)[j] = (q.v)[j], (q.v)[i] }
func (q *pipelineQueue) Push(x interface{}) {
	q.v = append(q.v, x.(*pipelineReq))
}
func (q *pipelineQueue) Pop() interface{} {
	r := q.v[q.Len()-1]
	q.v[q.Len()-1] = nil
	q.v = q.v[:q.Len()-1]
	return r
}
func (q *pipelineQueue) Peek() *pipelineReq {
	return q.v[0]
}

func newStreamPipeline(
	ctx context.Context,
	ss StreamStorage,
	flushQ chan<- chan bool) *streamPipeline {
	r := &streamPipeline{
		ss:      ss,
		flushQ:  flushQ,
		q:       new(pipelineQueue),
		qNotify: make(chan struct{}),
	}
	go r.Process(ctx)
	return r
}

func (p *streamPipeline) Process(ctx context.Context) {
	var maxId int64
	for {
	WaitLoop:
		for {
			head, queueNotify := p.peek()
			if head == nil {
				select {
				case <-ctx.Done():
					return
				case <-queueNotify:
				}
				continue
			}
			waitId := waitIdForRequest(head)
			if waitId <= maxId {
				break
			}
			tailId, tailNotify := p.ss.TailEntryId()
			maxId = tailId
			if waitId <= tailId {
				break
			}

			select {
			case <-ctx.Done():
				return
			case <-tailNotify:
			case <-queueNotify:
			case <-time.After(10 * time.Millisecond): // TODO(zviad): timeout constant
				break WaitLoop
			}
		}
		req := p.pop()
		var ok bool
		if req.R.Entry == nil || req.R.Entry.EntryId == 0 {
			ok = p.ss.CommitEntry(req.R.CommittedEntryId, req.R.CommittedEntryMd5)
			if ok && req.R.CommittedEntryId > maxId {
				maxId = req.R.CommittedEntryId
			}
		} else {
			isCommitted := req.R.CommittedEntryId >= req.R.Entry.EntryId
			ok = p.ss.PutEntry(req.R.Entry, isCommitted)
			if ok && req.R.Entry.EntryId > maxId {
				maxId = req.R.Entry.EntryId
			}
		}
		if !ok {
			req.okC <- false
		} else {
			p.flushQ <- req.okC
		}
	}
}

func (p *streamPipeline) Queue(r *walle_pb.PutEntryInternalRequest) <-chan bool {
	req := &pipelineReq{R: r, okC: make(chan bool, 1)}
	p.mx.Lock()
	defer p.mx.Unlock()
	heap.Push(p.q, req)
	if p.q.Peek() == req {
		close(p.qNotify)
		p.qNotify = make(chan struct{})
	}
	return req.okC
}

func (p *streamPipeline) peek() (*walle_pb.PutEntryInternalRequest, <-chan struct{}) {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.q.Len() == 0 {
		return nil, p.qNotify
	}
	return p.q.Peek().R, p.qNotify
}

func (p *streamPipeline) pop() *pipelineReq {
	p.mx.Lock()
	defer p.mx.Unlock()
	return heap.Pop(p.q).(*pipelineReq)
}

func waitIdForRequest(r *walle_pb.PutEntryInternalRequest) int64 {
	if r.Entry != nil && r.Entry.EntryId > 0 {
		return r.Entry.EntryId - 1
	}
	return r.CommittedEntryId
}
