package walle

import (
	"bytes"
	"context"
	"sync"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
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

// storagePipeline provides queue like abstraction to stream line
// put operations for each stream, and perform group FlushSync operations
// for much better overall throughput.
type storagePipeline struct {
	rootCtx             context.Context
	flushSync           func()
	flushQ              chan chan bool
	fetchCommittedEntry fetchFunc

	mx sync.Mutex
	p  map[string]*streamPipeline
}

func newStoragePipeline(
	ctx context.Context,
	flushSync func(),
	fetchCommittedEntry fetchFunc) *storagePipeline {
	r := &storagePipeline{
		rootCtx:             ctx,
		flushSync:           flushSync,
		flushQ:              make(chan chan bool, storageFlushQ),
		fetchCommittedEntry: fetchCommittedEntry,
		p:                   make(map[string]*streamPipeline),
	}
	go r.flusher(ctx)
	return r
}

func (s *storagePipeline) ForStream(ss StreamStorage) *streamPipeline {
	s.mx.Lock()
	defer s.mx.Unlock()
	p, ok := s.p[ss.StreamURI()]
	if !ok {
		p = newStreamPipeline(s.rootCtx, ss, s.flushQ, s.fetchCommittedEntry)
		s.p[ss.StreamURI()] = p
	}
	return p
}

func (s *storagePipeline) QueueFlush() <-chan bool {
	c := make(chan bool, 1)
	s.flushQ <- c
	return c
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
	ss                  StreamStorage
	flushQ              chan<- chan bool
	fetchCommittedEntry fetchFunc

	q *pipelineQueue
}

type pipelineReq struct {
	R   *walle_pb.PutEntryInternalRequest
	okC chan bool
}

type pipelineQueue struct {
	mx      sync.Mutex
	notifyC chan struct{}
	v       []*pipelineReq
}

func (q *pipelineQueue) Len() int {
	q.mx.Lock()
	defer q.mx.Unlock()
	return len(q.v)
}
func (q *pipelineQueue) notify() {
	close(q.notifyC)
	q.notifyC = make(chan struct{})
}
func (q *pipelineQueue) Pop() *pipelineReq {
	q.mx.Lock()
	defer q.mx.Unlock()
	r := q.v[0]
	copy(q.v, q.v[1:])
	q.v[len(q.v)-1] = nil
	q.v = q.v[:len(q.v)-1]
	q.notify()
	return r
}
func (q *pipelineQueue) CleanTillCommitted() bool {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.cleanTillCommitted()
}
func (q *pipelineQueue) cleanTillCommitted() bool {
	for idx := len(q.v) - 1; idx >= 0; idx-- {
		req := q.v[idx]
		if req.R.GetEntry().GetEntryId() > req.R.CommittedEntryId {
			continue
		}
		for k := 0; k < idx; k++ {
			q.v[k].okC <- false
			q.v[k] = nil
		}
		q.v = q.v[idx:]
		q.notify()
		return true
	}
	return false
}
func (q *pipelineQueue) Peek() (*walle_pb.PutEntryInternalRequest, <-chan struct{}) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if len(q.v) == 0 {
		return nil, q.notifyC
	}
	return q.v[0].R, q.notifyC
}
func (q *pipelineQueue) Push(r *walle_pb.PutEntryInternalRequest) <-chan bool {
	q.mx.Lock()
	defer q.mx.Unlock()
	waitId := waitIdForRequest(r)
	rIdx := 0
	for idx := len(q.v) - 1; idx >= 0; idx-- {
		req := q.v[idx]
		if req.R.GetEntry().GetEntryId() == r.GetEntry().GetEntryId() &&
			bytes.Compare(req.R.GetEntry().GetChecksumMd5(), r.GetEntry().GetChecksumMd5()) == 0 {
			if r.CommittedEntryId > req.R.CommittedEntryId {
				req.R.CommittedEntryId = r.CommittedEntryId
				req.R.CommittedEntryMd5 = r.CommittedEntryMd5
			}
			return req.okC
		}
		reqWaitId := waitIdForRequest(req.R)
		if reqWaitId > waitId ||
			(reqWaitId == waitId && req.R.GetEntry().GetEntryId() > r.GetEntry().GetEntryId()) {
			continue
		}
		rIdx = idx + 1
		break
	}
	req := &pipelineReq{R: r, okC: make(chan bool, 1)}
	q.v = append(q.v, nil)
	copy(q.v[rIdx+1:], q.v[rIdx:])
	q.v[rIdx] = req
	if len(q.v) > streamPipelineQ {
		zlog.Info(
			"DEBUG: pipeline is overflowing ",
			q.v[0].R.CommittedEntryId, " -- ", q.v[0].R.GetEntry().GetEntryId(), " -- ",
			r.CommittedEntryId, " -- ", r.GetEntry().GetEntryId(), " -- ",
			q.v[len(q.v)-1].R.CommittedEntryId, " -- ", q.v[len(q.v)-1].R.GetEntry().GetEntryId())
		q.cleanTillCommitted()
		q.notify()
	}
	if q.v[0] == req {
		q.notify()
	}
	return req.okC
}

func newStreamPipeline(
	ctx context.Context,
	ss StreamStorage,
	flushQ chan<- chan bool,
	fetchCommittedEntry fetchFunc) *streamPipeline {
	r := &streamPipeline{
		ss:                  ss,
		flushQ:              flushQ,
		fetchCommittedEntry: fetchCommittedEntry,
		q:                   &pipelineQueue{notifyC: make(chan struct{})},
	}
	go r.Process(ctx)
	return r
}

func (p *streamPipeline) waitForId(
	ctx context.Context, maxId int64) (int64, error) {
	for {
		head, queueNotify := p.q.Peek()
		if head == nil {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-queueNotify:
			}
			continue
		}
		waitId := waitIdForRequest(head)
		if waitId <= maxId {
			return maxId, nil
		}
		tailId, tailNotify := p.ss.TailEntryId()
		maxId = tailId
		if waitId <= tailId {
			return maxId, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-tailNotify:
		case <-queueNotify:
		case <-time.After(wallelib.LeaseMinimum / 4):
			if p.q.CleanTillCommitted() {
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
	panicOnNotOk(ok, "committed putEntry must always succeed")
	zlog.Infof("[sp] stream: %s caught up to: %d (might have created a gap)", p.ss.StreamURI(), entryId)
	return true
}

func (p *streamPipeline) Process(ctx context.Context) {
	var maxId int64
	for ctx.Err() == nil {
		var err error
		maxId, err = p.waitForId(ctx, maxId)
		if err != nil {
			return
		}
		req := p.q.Pop()
		var ok bool
		if req.R.GetEntry().GetEntryId() == 0 {
			ok = p.ss.CommitEntry(req.R.CommittedEntryId, req.R.CommittedEntryMd5)
			if !ok {
				ok = p.backfillEntry(ctx, req.R.CommittedEntryId, req.R.CommittedEntryMd5)
			}
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
		// zlog.Info(
		// 	"DEBUG: pipeline ", p.ss.StreamURI(), "-- ",
		// 	req.R.CommittedEntryId, " -- ", req.R.GetEntry().GetEntryId(), " -- ", maxId, " -- ", ok)
		if !ok {
			req.okC <- false
		} else {
			p.flushQ <- req.okC
		}
	}
}

func (p *streamPipeline) Queue(r *walle_pb.PutEntryInternalRequest) <-chan bool {
	return p.q.Push(r)
}

func waitIdForRequest(r *walle_pb.PutEntryInternalRequest) int64 {
	if r.GetEntry().GetEntryId() > 0 {
		return r.Entry.EntryId - 1
	}
	return r.CommittedEntryId
}
