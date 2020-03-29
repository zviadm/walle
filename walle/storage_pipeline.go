package walle

import (
	"bytes"
	"context"
	"sync"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
)

const (
	storageFlushQ   = 8192
	streamPipelineQ = 256
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

func (q *pipelineQueue) Len() int { return len(q.v) }
func (q *pipelineQueue) Pop() *pipelineReq {
	r := q.v[0]
	copy(q.v, q.v[1:])
	q.v = q.v[:len(q.v)-1]
	return r
}
func (q *pipelineQueue) Peek() *pipelineReq {
	return q.v[0]
}
func (q *pipelineQueue) Push(req *pipelineReq) bool {
	q.v = append(q.v, req)
	waitId := waitIdForRequest(req.R)
	for idx := len(q.v) - 2; idx >= 0; idx-- {
		r := q.v[idx]
		if r.R.CommittedEntryId == req.R.CommittedEntryId &&
			r.R.GetEntry().GetEntryId() == req.R.GetEntry().GetEntryId() &&
			bytes.Compare(r.R.GetEntry().GetChecksumMd5(), req.R.GetEntry().GetChecksumMd5()) == 0 {
			r.okC <- false
			q.v[idx] = req
			return (idx == 0)
		}
		if waitIdForRequest(r.R) >= waitId {
			q.v[idx+1], q.v[idx] = q.v[idx], q.v[idx+1]
			continue
		}
		break
	}
	if len(q.v) > streamPipelineQ {
		q.v[len(q.v)-1].okC <- false
		q.v = q.v[:len(q.v)-1]
	}
	return q.v[0] == req
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
	if p.q.Push(req) {
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
	return p.q.Pop()
}

func waitIdForRequest(r *walle_pb.PutEntryInternalRequest) int64 {
	if r.Entry != nil && r.Entry.EntryId > 0 {
		return r.Entry.EntryId - 1
	}
	return r.CommittedEntryId
}
