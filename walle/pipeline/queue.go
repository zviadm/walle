package pipeline

import (
	"math"
	"sort"
	"sync"

	"github.com/zviadm/stats-go/metrics"
	"github.com/zviadm/walle/walle/storage"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queue struct {
	mx            sync.Mutex
	v             map[int64]queueItem
	minId         int64 // Lower bound on smallest item in `v`.
	minIdToNotify int64 // If item with EntryId <= minIdToNotify shows up, send notify.
	// Notifications are sent if either minIdToNotify triggers or if any of max
	// committedId-s change.
	notifyC chan struct{}

	maxReadyCommittedId int64
	maxCommittedId      int64
	size                *atomic.Int64

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
	maxEntryId   = int64(math.MaxInt64 / 2) // avoid any accidental overflow bugs.
)

func newQueue(streamURI string, size *atomic.Int64) *queue {
	return &queue{
		minId:   maxEntryId,
		v:       make(map[int64]queueItem),
		notifyC: make(chan struct{}),
		size:    size,

		streamURI:  streamURI,
		sizeG:      queueSizeGauge.V(metrics.KV{"stream_uri": streamURI}),
		sizeBytesG: queueBytesGauge.V(metrics.KV{"stream_uri": streamURI}),
	}
}

func (q *queue) Close() {
	q.mx.Lock()
	defer q.mx.Unlock()
	q.size.Add(-int64(len(q.v)))
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

func (q *queue) IsEmpty() bool {
	q.mx.Lock()
	defer q.mx.Unlock()
	return len(q.v) == 0
}

func (q *queue) MaxCommittedId() (int64, <-chan struct{}) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if q.maxCommittedId <= q.maxReadyCommittedId {
		return 0, q.notifyC
	}
	return q.maxCommittedId, q.notifyC
}
func (q *queue) MaxReadyCommittedId() (int64, <-chan struct{}) {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.maxReadyCommittedId, q.notifyC
}
func (q *queue) EntryXX(entryId int64) (uint64, bool) {
	q.mx.Lock()
	defer q.mx.Unlock()
	item, ok := q.v[entryId]
	if !ok {
		return 0, false
	}
	return item.R.EntryXX, true
}

func (q *queue) PopReady(tailId int64, forceSkip bool, r []queueItem) ([]queueItem, <-chan struct{}) {
	q.mx.Lock()
	defer q.mx.Unlock()
	q.minIdToNotify = tailId + 1
	if r != nil {
		r = r[:0]
	}
	if len(q.v) == 0 {
		return r, q.notifyC
	}
	popTillId := tailId
	for {
		item, ok := q.v[popTillId+1]
		if !ok || item.R.Entry == nil {
			break
		}
		popTillId += 1
	}
	r = q.popEntriesTill(r, popTillId)
	if len(r) == 0 && forceSkip {
		popTillId := maxEntryId
		item, ok := q.v[q.maxReadyCommittedId]
		if ok {
			q.remove(item.R)
			r = append(r, item)
			popTillId = item.R.EntryId
		}
		r = q.popEntriesTill(r, popTillId)
	}
	return r, q.notifyC
}
func (q *queue) popEntriesTill(r []queueItem, endId int64) []queueItem {
	if endId < q.minId {
		return r
	}
	rangeN := endId - q.minId + 1
	if rangeN > 10 && rangeN > int64(len(q.v)) {
		// Slow path, this should be very rare. This can happen if there is a large stall
		// that causes entries to arrive greatly out of order.
		for entryId, item := range q.v {
			if entryId > endId {
				continue
			}
			q.remove(item.R)
			r = append(r, item)
		}
		sort.Slice(r, func(i, j int) bool { return r[i].R.EntryId < r[j].R.EntryId })
	} else {
		for entryId := q.minId; entryId <= endId; entryId++ {
			item, ok := q.v[entryId]
			if !ok {
				continue
			}
			q.remove(item.R)
			r = append(r, item)
		}
	}
	q.minId = endId + 1
	return r
}
func (q *queue) remove(r *request) {
	delete(q.v, r.EntryId)
	q.size.Add(-1)
	q.sizeG.Add(-1)
	q.sizeBytesG.Add(-float64(len(r.Entry.GetData())))
}

func (q *queue) Queue(r *request) *ResultCtx {
	q.mx.Lock()
	defer q.mx.Unlock()
	shouldNotify := len(q.v) == 0
	item, ok := q.v[r.EntryId]
	if !ok {
		res := newResult()
		item = queueItem{R: r, Res: res}
		q.size.Add(1)
		q.sizeG.Add(1)
		q.sizeBytesG.Add(float64(len(r.Entry.GetData())))
	} else {
		item.R.Committed = item.R.Committed || r.Committed
		if r.Entry != nil {
			prevEntry := item.R.Entry
			if item.R.Entry == nil {
				item.R.Entry = r.Entry
			} else {
				writerCmp := storage.CmpWriterIds(item.R.Entry.WriterId, r.Entry.WriterId)
				if writerCmp < 0 {
					item.Res.set(status.Errorf(codes.FailedPrecondition,
						"%s: %v < %v", q.streamURI, item.R.Entry.WriterId, r.Entry.WriterId))
					item.Res = newResult()
					item.R.Entry = r.Entry
				} else if writerCmp > 0 {
					return newResultWithErr(
						status.Errorf(codes.FailedPrecondition,
							"%s: %v < %v", q.streamURI, r.Entry.WriterId, item.R.Entry.WriterId))
				}
			}
			sizeDelta := len(r.Entry.Data) - len(prevEntry.GetData())
			q.sizeBytesG.Add(float64(sizeDelta))
		}
	}
	q.v[item.R.EntryId] = item
	if item.R.EntryId < q.minId {
		q.minId = item.R.EntryId
	}
	if item.R.Committed && (item.R.EntryId > q.maxCommittedId) {
		q.maxCommittedId = item.R.EntryId
		shouldNotify = true
	}
	if item.R.Committed && item.R.Entry != nil && (item.R.EntryId > q.maxReadyCommittedId) {
		q.maxReadyCommittedId = item.R.EntryId
		shouldNotify = true
	}
	if shouldNotify || item.R.EntryId <= q.minIdToNotify {
		q.notify()
	}
	return item.Res
}
