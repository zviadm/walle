package wallelib

import (
	"context"
	"sync"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"google.golang.org/grpc/status"
)

// processor is a complex queue executor that tries to send requests
// in parallel but also in EntryId order.
//
// processor maintains two main queues, regular request queue where new
// requests are placed and retry queue that holds requests that fail.
// In steady state, processor sends requests all in parallel while maintaing
// limit of maximum requests in-flight. If requests start failing and retry
// queue gets items in it, processor will attempt to retry requests again
// in correct EntryId order.
type processor struct {
	ctx             context.Context
	cancelCtx       func(err error)
	newPutter       func() (entryPutter, error)
	maxInflightN    int
	maxInflightSize int

	reqQmx     sync.Mutex
	reqQ       []*PutCtx
	reqQNotify chan struct{}

	inflightWG sync.WaitGroup
	resultQ    chan putCtxAndErr

	// variables below are only accessed in processLoop go routine.
	primaryQ     []*PutCtx
	retryQ       []*PutCtx
	inflightN    int
	inflightSize int
	idxByEntry   map[int64]int // EntryId -> Send IDX

	putter         entryPutter
	putterFirstIdx int
}

type entryPutter interface {
	IsPreferred() bool
	Put(ctx context.Context, entry *walleapi.Entry) error
}

type putCtxAndErr struct {
	P *PutCtx
	E error
}

func newProcessor(
	ctx context.Context,
	cancelCtx func(err error),
	newPutter func() (entryPutter, error),
	maxInflightN int,
	maxInflightSize int) *processor {
	p := &processor{
		ctx:             ctx,
		cancelCtx:       cancelCtx,
		newPutter:       newPutter,
		maxInflightN:    maxInflightN,
		maxInflightSize: maxInflightSize,
		reqQNotify:      make(chan struct{}, 1),
		resultQ:         make(chan putCtxAndErr, maxInflightN),
		idxByEntry:      make(map[int64]int, maxInflightN),
	}
	go p.processLoop()
	return p
}

// Queue queues new request for processor to handle. Processor assumes that
// requests are coming in increasing EntryId order. This call is NOT thread
// safe.
func (p *processor) Queue(r *PutCtx) {
	p.reqQmx.Lock()
	defer p.reqQmx.Unlock()
	if err := p.ctx.Err(); err != nil {
		r.set(err)
		return
	}
	p.reqQ = append(p.reqQ, r)
	select {
	case p.reqQNotify <- struct{}{}:
	default:
	}
}

// Cleans up all queued or inflight requests to make sure all items
// that have been scheduled get canceled and all PutCtx-es get resolved.
func (p *processor) cleanup() {
	if p.ctx.Err() == nil {
		panic("cleanup mustn't be called until context is closed")
	}
	p.inflightWG.Wait()
	close(p.resultQ)
	for r := range p.resultQ {
		r.P.set(p.ctx.Err())
	}
	for _, req := range p.primaryQ {
		req.set(p.ctx.Err())
	}
	for _, r := range p.retryQ {
		r.set(p.ctx.Err())
	}
	p.reqQmx.Lock()
	defer p.reqQmx.Unlock()
	for _, req := range p.reqQ {
		req.set(p.ctx.Err())
	}
}

// refreshPutter makes sure putter is initialized and ideally IsPreferred().
func (p *processor) refreshPutter(sendIdx int) error {
	for p.putter == nil || !p.putter.IsPreferred() {
		var err error
		p.putter, err = p.newPutter()
		if err != nil {
			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			case res := <-p.resultQ:
				p.handleResult(res)
			case <-time.After(connectTimeout):
			}
			continue
		}
		p.putterFirstIdx = sendIdx
	}
	return nil
}

func (p *processor) processLoop() {
	defer p.cleanup()

	// sendIdx is monotonically increasing number, used to identify
	// order in which Put requests were made for different requests.
	sendIdx := 0
	for ; p.ctx.Err() == nil; sendIdx++ {
		if err := p.refreshPutter(sendIdx); err != nil {
			continue
		}
		if len(p.primaryQ) == 0 && len(p.retryQ) == 0 {
			p.waitOnQs()
			continue
		}
		req := p.pickNext()
		if req == nil {
			select {
			case <-p.ctx.Done():
			case res := <-p.resultQ:
				p.handleResult(res)
			}
			continue
		}
		p.idxByEntry[req.Entry.EntryId] = sendIdx
		p.inflightWG.Add(1)
		p.inflightN += 1
		p.inflightSize += req.Entry.Size()
		go func(c entryPutter, req *PutCtx) {
			err := c.Put(p.ctx, req.Entry)
			p.resultQ <- putCtxAndErr{P: req, E: err}
			p.inflightWG.Done()
		}(p.putter, req)
	}
}

// waitOnQs waits for either new items to appear in resultQ or
// for new work to show up in reqQ.
func (p *processor) waitOnQs() {
	select {
	case <-p.ctx.Done():
	case res := <-p.resultQ:
		p.handleResult(res)
	case <-p.reqQNotify:
		p.reqQmx.Lock()
		defer p.reqQmx.Unlock()
		p.primaryQ, p.reqQ = p.reqQ, nil
	}
	return
}

// pickNext picks next request to perform. retryQ gets the priority,
// otherwise item will be picked from primaryQ.
// Can also return `nil` if there are no requests that can be performed at this time.
func (p *processor) pickNext() *PutCtx {
	if len(p.retryQ) > 0 {
		req := p.retryQ[0]
		prevIdx, ok := p.idxByEntry[req.Entry.EntryId-1]
		if ok && prevIdx < p.idxByEntry[req.Entry.EntryId] {
			return nil
		}
		p.retryQ[0] = nil
		p.retryQ = p.retryQ[1:]
		return req
	}

	// check inflight requirements.
	req := p.primaryQ[0]
	if p.inflightN+1 > p.maxInflightN ||
		p.inflightSize+req.Entry.Size() > p.maxInflightSize {
		return nil
	}
	p.primaryQ[0] = nil
	p.primaryQ = p.primaryQ[1:]
	return req
}

// handleResult handles result from Put call. If request fails with a retriable
// error, puts it back on the retryQ to attempt it again.
func (p *processor) handleResult(r putCtxAndErr) {
	p.inflightN -= 1
	p.inflightSize -= r.P.Entry.Size()
	if r.E == nil {
		r.P.set(nil)
		delete(p.idxByEntry, r.P.Entry.EntryId)
		return
	}
	if IsErrFinal(status.Convert(r.E).Code()) {
		r.P.set(r.E)
		p.cancelCtx(r.E)
		return
	}
	p.retryQ = append(p.retryQ, r.P)
	// Perform insert-sort on p.retryQ to keep it sorted by EntryId.
	for idx := len(p.retryQ) - 2; idx >= 0; idx-- {
		if p.retryQ[idx].Entry.EntryId < p.retryQ[idx+1].Entry.EntryId {
			break
		}
		p.retryQ[idx], p.retryQ[idx+1] = p.retryQ[idx+1], p.retryQ[idx]
	}
	if p.idxByEntry[r.P.Entry.EntryId] >= p.putterFirstIdx {
		// Reset putter if this request was sent after putter was refreshed/initialized.
		// That means, this putter was the one that errored out for this request.
		p.putter = nil
	}
}
