package pipeline

import (
	"bytes"
	"sync"

	"github.com/zviadm/walle/walle/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queue struct {
	mx      sync.Mutex
	tailId  int64
	v       map[int64]*queueItem
	notifyC chan struct{}

	maxReadyCommittedId int64
	maxCommittedId      int64

	sizeDataB int
	maxSizeB  int
}

type queueItem struct {
	R   *Request
	Res *ResultCtx
}

const (
	itemOverhead = 128
	maxQueueLen  = 2048
)

func newQueue(maxSizeB int) *queue {
	return &queue{
		maxSizeB: maxSizeB,
		v:        make(map[int64]*queueItem),
		notifyC:  make(chan struct{}),
	}
}

func (q *queue) notify() {
	close(q.notifyC)
	q.notifyC = make(chan struct{})
}

func (q *queue) CanSkip() bool {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.maxCommittedId > q.tailId
}

func (q *queue) PopReady(tailId int64, forceSkip bool) ([]*queueItem, chan struct{}) {
	q.mx.Lock()
	defer q.mx.Unlock()
	prevTailId := q.tailId
	q.tailId = tailId
	if len(q.v) == 0 {
		return nil, q.notifyC
	}
	var r []*queueItem
	r = q.popTillTail(r, prevTailId)
	item, ok := q.v[tailId+1]
	if ok && item.R.IsReady(tailId) {
		delete(q.v, item.R.EntryId)
		r = append(r, item)
	}
	if len(r) == 0 && q.maxCommittedId > q.tailId && (forceSkip || q.isOverflowing()) {
		item, ok := q.v[q.maxReadyCommittedId]
		if !ok {
			item, ok = q.v[q.maxCommittedId]
		}
		if ok {
			delete(q.v, item.R.EntryId)
			r = append(r, item)
			prevTailId = q.tailId
			q.tailId = item.R.EntryId
			r = q.popTillTail(r, prevTailId)
		}
	}
	return r, q.notifyC
}
func (q *queue) popTillTail(r []*queueItem, prevTailId int64) []*queueItem {
	if q.tailId-prevTailId > int64(len(q.v)) {
		for entryId, i := range q.v {
			if entryId > q.tailId {
				continue
			}
			delete(q.v, entryId)
			r = append(r, i)
		}
	} else {
		for entryId := prevTailId + 1; entryId <= q.tailId; entryId++ {
			item, ok := q.v[entryId]
			if ok {
				delete(q.v, item.R.EntryId)
				r = append(r, item)
			}
		}
	}
	return r
}

func (q *queue) Queue(r *Request) (*ResultCtx, bool) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if r.EntryId <= q.tailId {
		return nil, false
	}
	qItem, ok := q.v[r.EntryId]
	if !ok {
		res := newResult()
		qItem = &queueItem{R: r, Res: res}
		q.sizeDataB += len(r.Entry.GetData())
		q.v[r.EntryId] = qItem
	} else {
		qItem.R.Committed = qItem.R.Committed || r.Committed
		if r.Entry != nil {
			if qItem.R.Entry == nil {
				qItem.R.Entry = r.Entry
			} else {
				writerIdCmp := bytes.Compare(qItem.R.Entry.WriterId, r.Entry.WriterId)
				if writerIdCmp < 0 {
					qItem.Res.set(status.Errorf(codes.FailedPrecondition,
						"%s < %s", storage.WriterId(qItem.R.Entry.WriterId), storage.WriterId(r.Entry.WriterId)))
					q.sizeDataB += len(r.Entry.Data) - len(qItem.R.Entry.Data)
					qItem.Res = newResult()
					qItem.R.Entry = r.Entry
				} else if writerIdCmp > 0 {
					res := newResult()
					res.set(status.Errorf(codes.FailedPrecondition,
						"%s < %s", storage.WriterId(r.Entry.WriterId), storage.WriterId(qItem.R.Entry.WriterId)))
					return res, true
				}
			}
		}
	}
	if qItem.R.IsReady(q.tailId) || qItem.R.Committed || q.isOverflowing() {
		q.notify()
	}
	if r.Committed && (r.EntryId > q.maxCommittedId) {
		q.maxCommittedId = r.EntryId
	}
	if r.Committed && r.Entry != nil && (r.EntryId > q.maxReadyCommittedId) {
		q.maxReadyCommittedId = r.EntryId
	}
	return qItem.Res, true
}

func (q *queue) isOverflowing() bool {
	return len(q.v) > maxQueueLen ||
		(len(q.v)*itemOverhead+q.sizeDataB) >= q.maxSizeB
}
