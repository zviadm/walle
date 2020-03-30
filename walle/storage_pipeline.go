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
	for idx := len(q.v) - 1; idx >= 0; idx-- {
		if q.v[idx].R.GetEntry().GetEntryId() > q.v[idx].R.CommittedEntryId {
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
		q.v[len(q.v)-1].okC <- false
		q.v[len(q.v)-1] = nil
		q.v = q.v[:len(q.v)-1]
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

func (p *streamPipeline) Process(ctx context.Context) {
	var maxId int64
	for {
	WaitLoop:
		for {
			head, queueNotify := p.q.Peek()
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
			case <-time.After(wallelib.LeaseMinimum / 8):
				if p.q.CleanTillCommitted() {
					break WaitLoop
				}
				// zlog.Info(
				// 	"DEBUG: pipeline is stuck? ",
				// 	p.q.Len(), " -- ", head.CommittedEntryId, " -- ", head.GetEntry().GetEntryId(), " -- ", maxId)
			}
		}
		req := p.q.Pop()
		var ok bool
		if req.R.GetEntry().GetEntryId() == 0 {
			ok = p.ss.CommitEntry(req.R.CommittedEntryId, req.R.CommittedEntryMd5)
			if !ok {
				// Try to fetch the committed entry from other servers and create a GAP locally to continue with
				// the put.
				ctx, cancel := context.WithTimeout(context.Background(), time.Second) // TODO(zviad): timeout constant?
				defer cancel()
				entry, err := p.fetchCommittedEntry(
					ctx, p.ss.StreamURI(), req.R.CommittedEntryId, req.R.CommittedEntryMd5)
				if err != nil {
					zlog.Warningf("[sp] err fetching: %s:%d - %s", p.ss.StreamURI(), req.R.CommittedEntryId, err)
				} else {
					ok = p.ss.PutEntry(entry, true)
					zlog.Infof(
						"[sp] stream: %s caught up to: %d (might have created a gap)",
						p.ss.StreamURI(), req.R.CommittedEntryId)
				}
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
