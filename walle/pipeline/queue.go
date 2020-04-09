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
	tailID  int64
	v       map[int64]queueItem
	notifyC chan struct{}

	maxReadyCommittedID int64
	maxCommittedID      int64

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
		v:        make(map[int64]queueItem),
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
	return q.maxCommittedID > q.tailID
}

func (q *queue) PopReady(tailID int64, forceSkip bool) ([]queueItem, chan struct{}) {
	q.mx.Lock()
	defer q.mx.Unlock()
	prevTailID := q.tailID
	q.tailID = tailID
	if len(q.v) == 0 {
		return nil, q.notifyC
	}
	var r []queueItem
	r = q.popTillTail(r, prevTailID)
	item, ok := q.v[tailID+1]
	if ok && item.R.IsReady(tailID) {
		delete(q.v, item.R.EntryId)
		r = append(r, item)
	}
	if len(r) == 0 && q.maxCommittedID > q.tailID && (forceSkip || q.isOverflowing()) {
		item, ok := q.v[q.maxReadyCommittedID]
		if !ok {
			item, ok = q.v[q.maxCommittedID]
		}
		if ok {
			delete(q.v, item.R.EntryId)
			r = append(r, item)
			prevTailID = q.tailID
			q.tailID = item.R.EntryId
			r = q.popTillTail(r, prevTailID)
		}
	}
	return r, q.notifyC
}
func (q *queue) popTillTail(r []queueItem, prevTailID int64) []queueItem {
	if q.tailID-prevTailID > int64(len(q.v)) {
		for entryID, i := range q.v {
			if entryID > q.tailID {
				continue
			}
			delete(q.v, entryID)
			r = append(r, i)
		}
	} else {
		for entryID := prevTailID + 1; entryID <= q.tailID; entryID++ {
			item, ok := q.v[entryID]
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
	if r.EntryId <= q.tailID {
		return nil, false
	}
	qItem, ok := q.v[r.EntryId]
	if !ok {
		res := newResult()
		qItem = queueItem{R: r, Res: res}
		q.sizeDataB += len(r.Entry.GetData())
	} else {
		qItem.R.Committed = qItem.R.Committed || r.Committed
		if r.Entry != nil {
			if qItem.R.Entry == nil {
				qItem.R.Entry = r.Entry
			} else {
				writerCmp := bytes.Compare(qItem.R.Entry.WriterId, r.Entry.WriterId)
				if writerCmp < 0 {
					qItem.Res.set(status.Errorf(codes.FailedPrecondition,
						"%s < %s", storage.WriterId(qItem.R.Entry.WriterId), storage.WriterId(r.Entry.WriterId)))
					q.sizeDataB += len(r.Entry.Data) - len(qItem.R.Entry.Data)
					qItem.Res = newResult()
					qItem.R.Entry = r.Entry
				} else if writerCmp > 0 {
					res := newResult()
					res.set(status.Errorf(codes.FailedPrecondition,
						"%s < %s", storage.WriterId(r.Entry.WriterId), storage.WriterId(qItem.R.Entry.WriterId)))
					return res, true
				}
			}
		}
	}
	q.v[r.EntryId] = qItem
	if qItem.R.IsReady(q.tailID) || qItem.R.Committed || q.isOverflowing() {
		q.notify()
	}
	if r.Committed && (r.EntryId > q.maxCommittedID) {
		q.maxCommittedID = r.EntryId
	}
	if r.Committed && r.Entry != nil && (r.EntryId > q.maxReadyCommittedID) {
		q.maxReadyCommittedID = r.EntryId
	}
	return qItem.Res, true
}

func (q *queue) isOverflowing() bool {
	return len(q.v) > maxQueueLen ||
		(len(q.v)*itemOverhead+q.sizeDataB) >= q.maxSizeB
}
