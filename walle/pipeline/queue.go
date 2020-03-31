package pipeline

import (
	"bytes"
	"context"
	"sync"
)

type queue struct {
	mx      sync.Mutex
	notifyC chan struct{}
	v       []*queueItem
}

type queueItem struct {
	R   *Request
	Res *ResultCtx
}

func newQueue() *queue {
	return &queue{notifyC: make(chan struct{})}
}

func (q *queue) Len() int {
	q.mx.Lock()
	defer q.mx.Unlock()
	return len(q.v)
}
func (q *queue) notify() {
	close(q.notifyC)
	q.notifyC = make(chan struct{})
}
func (q *queue) Pop() *queueItem {
	q.mx.Lock()
	defer q.mx.Unlock()
	r := q.v[0]
	copy(q.v, q.v[1:])
	q.v[len(q.v)-1] = nil
	q.v = q.v[:len(q.v)-1]
	q.notify()
	return r
}
func (q *queue) Peek() (*Request, <-chan struct{}) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if len(q.v) == 0 {
		return nil, q.notifyC
	}
	rCopy := *(q.v[0].R)
	return &rCopy, q.notifyC
}
func (q *queue) Queue(r *Request) *ResultCtx {
	q.mx.Lock()
	defer q.mx.Unlock()
	rIdx := len(q.v)
	for idx := 0; idx < len(q.v); idx++ {
		if q.v[idx].R.EntryId == r.EntryId &&
			bytes.Compare(q.v[idx].R.EntryMd5, r.EntryMd5) == 0 {
			if r.Entry != nil {
				q.v[idx].R.Entry = r.Entry
			}
			q.v[idx].R.Committed = q.v[idx].R.Committed || r.Committed
			return q.v[idx].Res
		}
		if q.v[idx].R.EntryId > r.EntryId {
			rIdx = idx
			break
		}
	}
	q.v = append(q.v, nil)
	copy(q.v[rIdx+1:], q.v[rIdx:])
	res := newResult()
	q.v[rIdx] = &queueItem{R: r, Res: res}
	if rIdx == 0 || len(q.v) > streamPipelineQ {
		q.notify()
	}
	return res
}

func (q *queue) PopTillCommitted() bool {
	q.mx.Lock()
	defer q.mx.Unlock()
	committedIdx := -1
	for idx := 0; idx < len(q.v); idx++ {
		req := q.v[idx]
		if req.R.Committed {
			committedIdx = idx
			if req.R.Entry != nil {
				break
			}
		}
	}
	if committedIdx < 0 {
		return false
	}
	for k := 0; k < committedIdx; k++ {
		q.v[k].Res.set(context.DeadlineExceeded)
		q.v[k] = nil
	}
	copy(q.v, q.v[committedIdx:])
	q.v = q.v[:len(q.v)-committedIdx]
	q.notify()
	return true
}
