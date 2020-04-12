package pipeline

import (
	"bytes"
	"math"
	"sync"

	"github.com/zviadm/stats-go/metrics"
	"github.com/zviadm/walle/walle/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queue struct {
	mx      sync.Mutex
	tailId  int64
	v       map[int64]queueItem
	notifyC chan struct{}

	maxReadyCommittedId int64
	maxCommittedId      int64

	streamURI  string
	sizeG      metrics.Gauge
	sizeBytesG metrics.Gauge
}

type queueItem struct {
	R   *request
	Res *ResultCtx
}

const (
	itemOverhead = 128
)

func newQueue(streamURI string) *queue {
	return &queue{
		v:       make(map[int64]queueItem),
		notifyC: make(chan struct{}),

		sizeG:      queueSizeGauge.V(metrics.KV{"stream_uri": streamURI}),
		sizeBytesG: queueBytesGauge.V(metrics.KV{"stream_uri": streamURI}),
	}
}

func (q *queue) Close() {
	q.mx.Lock()
	defer q.mx.Unlock()
	q.sizeG.Add(-float64(len(q.v)))
	for _, item := range q.v {
		item.Res.set(status.Errorf(codes.NotFound, "%s closed", q.streamURI))
		q.remove(item.R)
	}
}

func (q *queue) notify() {
	close(q.notifyC)
	q.notifyC = make(chan struct{})
}

func (q *queue) CanSkip() bool {
	q.mx.Lock()
	defer q.mx.Unlock()
	return len(q.v) > 0
}

func (q *queue) PopReady(tailId int64, forceSkip bool, r []queueItem) ([]queueItem, chan struct{}) {
	q.mx.Lock()
	defer q.mx.Unlock()
	prevTailId := q.tailId
	q.tailId = tailId
	if len(q.v) == 0 {
		return nil, q.notifyC
	}
	if r != nil {
		r = r[:0]
	}
	r = q.popEntries(r, prevTailId+1, q.tailId)
	item, ok := q.v[tailId+1]
	if ok && item.R.IsReady(tailId) {
		q.remove(item.R)
		r = append(r, item)
	}
	if len(r) == 0 && forceSkip {
		popTillId := int64(math.MaxInt64)
		item, ok := q.v[q.maxCommittedId]
		if ok {
			itemReady, ok := q.v[q.maxReadyCommittedId]
			if ok {
				item = itemReady
			}
			q.remove(item.R)
			r = append(r, item)
			popTillId = item.R.EntryId
		}
		r = q.popEntries(r, q.tailId+1, popTillId)
	}
	return r, q.notifyC
}
func (q *queue) popEntries(r []queueItem, startId int64, endId int64) []queueItem {
	if startId-endId+1 > int64(len(q.v)) {
		for entryId, item := range q.v {
			if entryId > endId {
				continue
			}
			q.remove(item.R)
			r = append(r, item)
		}
	} else {
		for entryId := startId; entryId <= endId; entryId++ {
			item, ok := q.v[entryId]
			if !ok {
				continue
			}
			q.remove(item.R)
			r = append(r, item)
		}
	}
	return r
}
func (q *queue) remove(r *request) {
	delete(q.v, r.EntryId)
	q.sizeBytesG.Add(-float64(len(r.Entry.GetData())))
	q.sizeG.Add(-1)
}

func (q *queue) Queue(r *request) (*ResultCtx, bool) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if r.EntryId <= q.tailId {
		return nil, false
	}
	wasEmpty := len(q.v) == 0
	qItem, ok := q.v[r.EntryId]
	if !ok {
		res := newResult()
		qItem = queueItem{R: r, Res: res}
		q.sizeBytesG.Add(float64(len(r.Entry.GetData())))
		q.sizeG.Add(1)
	} else {
		qItem.R.Committed = qItem.R.Committed || r.Committed
		if r.Entry != nil {
			prevEntry := qItem.R.Entry
			if qItem.R.Entry == nil {
				qItem.R.Entry = r.Entry
			} else {
				writerCmp := bytes.Compare(qItem.R.Entry.WriterId, r.Entry.WriterId)
				if writerCmp < 0 {
					qItem.Res.set(status.Errorf(codes.FailedPrecondition,
						"%s: %s < %s", q.streamURI, storage.WriterId(qItem.R.Entry.WriterId), storage.WriterId(r.Entry.WriterId)))
					qItem.Res = newResult()
					qItem.R.Entry = r.Entry
				} else if writerCmp > 0 {
					res := newResult()
					res.set(status.Errorf(codes.FailedPrecondition,
						"%s: %s < %s", q.streamURI, storage.WriterId(r.Entry.WriterId), storage.WriterId(qItem.R.Entry.WriterId)))
					return res, true
				}
			}
			sizeDelta := len(r.Entry.Data) - len(prevEntry.GetData())
			q.sizeBytesG.Add(float64(sizeDelta))
		}
	}
	q.v[r.EntryId] = qItem
	if wasEmpty || qItem.R.IsReady(q.tailId) || qItem.R.Committed {
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
