package pipeline

import (
	"bytes"
	"sync"
)

type queue struct {
	mx             sync.Mutex
	notifyC        chan struct{}
	v              []*queueItem
	maxCommittedId int64

	sizeDataB int
	maxSizeB  int
}

type queueItem struct {
	R   *Request
	Res *ResultCtx
}

const (
	itemOverhead = 128
)

func newQueue(maxSizeB int) *queue {
	return &queue{
		maxSizeB: maxSizeB,
		notifyC:  make(chan struct{}),
	}
}

// func (q *queue) Len() int {
// 	q.mx.Lock()
// 	defer q.mx.Unlock()
// 	return len(q.v)
// }
func (q *queue) notify() {
	close(q.notifyC)
	q.notifyC = make(chan struct{})
}
func (q *queue) PopReady(maxId int64) *queueItem {
	q.mx.Lock()
	defer q.mx.Unlock()
	if len(q.v) == 0 {
		return nil
	}

	rIdx := len(q.v) - 1
	if q.maxCommittedId > maxId {
		for idx := len(q.v) - 1; idx >= 0; idx-- {
			if q.v[idx].R.IsReady(maxId) {
				rIdx = idx
				break
			}
			if q.v[idx].R.Committed {
				rIdx = idx
			}
		}
	}
	r := q.v[rIdx]
	copy(q.v[rIdx:], q.v[rIdx+1:])
	q.v[len(q.v)-1] = nil
	q.v = q.v[:len(q.v)-1]
	q.sizeDataB -= len(r.R.Entry.GetData())
	return r
}
func (q *queue) Peek() (*Request, <-chan struct{}) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if len(q.v) == 0 {
		return nil, q.notifyC
	}
	rCopy := *(q.v[len(q.v)-1].R)
	return &rCopy, q.notifyC
}
func (q *queue) Queue(r *Request) *ResultCtx {
	q.mx.Lock()
	defer q.mx.Unlock()
	if r.Committed && r.EntryId > q.maxCommittedId {
		q.maxCommittedId = r.EntryId
	}
	rIdx := 0
	for idx := len(q.v) - 1; idx >= 0; idx-- {
		if q.v[idx].R.EntryId == r.EntryId &&
			bytes.Compare(q.v[idx].R.EntryMd5, r.EntryMd5) == 0 {
			if r.Entry != nil && q.v[idx].R.Entry == nil {
				q.v[idx].R.Entry = r.Entry
				q.sizeDataB += len(r.Entry.Data)
			}
			q.v[idx].R.Committed = q.v[idx].R.Committed || r.Committed
			return q.v[idx].Res
		}
		if q.v[idx].R.EntryId > r.EntryId {
			rIdx = idx + 1
			break
		}
	}
	q.v = append(q.v, nil)
	copy(q.v[rIdx+1:], q.v[rIdx:])
	res := newResult()
	q.v[rIdx] = &queueItem{R: r, Res: res}
	q.sizeDataB += len(r.Entry.GetData())
	if rIdx == len(q.v)-1 || q.isOverflowing() {
		q.notify()
	}
	return res
}
func (q *queue) MaxCommittedId() int64 {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.maxCommittedId
}
func (q *queue) IsOverflowing() bool {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.isOverflowing()
}
func (q *queue) isOverflowing() bool {
	return (len(q.v)*itemOverhead + q.sizeDataB) >= q.maxSizeB
}
