package pipeline

import (
	"context"
	"time"

	"github.com/zviadm/stats-go/metrics"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/zlog"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// QueueMaxTimeout represents max amount of time that items can stay in the queue.
	// This bounds queue size.
	QueueMaxTimeout = time.Second

	// maxTotalBacklog is maximum items allwoed to be queued across all streams. After this
	// limit is hit, it is important to start rejecting requests right away to avoid huge
	// performance drop, due to GC and memory pressure.
	// TODO(zviadm): This could be a configurable using a flag.
	maxTotalBacklog = 128 * 1024
)

type stream struct {
	ss                  storage.Stream
	fetchCommittedEntry fetchFunc
	q                   *queue
	totalQ              *atomic.Int64
	fforwardsC          metrics.Counter
}

func newStream(
	ctx context.Context,
	ss storage.Stream,
	fetchCommittedEntry fetchFunc,
	totalQ *atomic.Int64) *stream {
	r := &stream{
		ss:                  ss,
		fetchCommittedEntry: fetchCommittedEntry,
		q:                   newQueue(ss.StreamURI(), totalQ),
		totalQ:              totalQ,
		fforwardsC:          fforwardsCounter.V(metrics.KV{"stream_uri": ss.StreamURI()}),
	}
	go r.fastForward(ctx)
	go r.process(ctx)
	return r
}

// fastForward watches if a committed entry is stuck in queue for too long, and if it is
// will attempt to fetch it from other servers and fast forward to it. This can happen if
// this server misses PutEntry calls and needs to catch up to other servers by creating a gap.
func (p *stream) fastForward(ctx context.Context) {
	for ctx.Err() == nil {
		committedId, notify := p.q.MaxCommittedId()
		if committedId <= p.ss.TailEntryId() {
			select {
			case <-ctx.Done():
				return
			case <-notify:
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.skipTimeoutAdjusted()):
		}
		if committedId <= p.ss.TailEntryId() {
			continue
		}
		committedXX, ok := p.q.EntryXX(committedId)
		if !ok {
			continue
		}
		fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		entry, err := p.fetchCommittedEntry(
			fetchCtx, p.ss.StreamURI(), committedId, committedXX)
		cancel()
		if err != nil {
			zlog.Infof("[ff] stream: %s err fetching - %s", p.ss.StreamURI(), err)
			continue
		}
		p.fforwardsC.Count(1)
		zlog.Infof("[ff] stream: %s, fforward to: %d", p.ss.StreamURI(), entry.EntryId)
		_ = p.QueuePut(entry, true)
	}
}

func (p *stream) skipTimeoutAdjusted() time.Duration {
	timeout := QueueMaxTimeout
	_, _, writerLease, _ := p.ss.WriterInfo()
	if writerLease > 0 && writerLease/4 < timeout {
		timeout = writerLease / 4
	}
	return timeout
}

func (p *stream) process(ctx context.Context) {
	defer p.q.Close()
	forceSkip := false
	var maxTimeout <-chan time.Time
	var skipTimeout <-chan time.Time
	var reqs []queueItem
	var qNotify <-chan struct{}
	for ctx.Err() == nil && !p.ss.IsClosed() {
		reqs, qNotify = p.q.PopReady(p.ss.TailEntryId(), forceSkip, reqs)
		if len(reqs) == 0 {
			if skipTimeout == nil {
				readyId, _ := p.q.MaxReadyCommittedId()
				if readyId > p.ss.TailEntryId() {
					skipTimeout = time.After(p.skipTimeoutAdjusted())
				}
			}
			if maxTimeout == nil && !p.q.IsEmpty() {
				maxTimeout = time.After(QueueMaxTimeout)
			}
			select {
			case <-ctx.Done():
				return
			case <-qNotify:
			case <-skipTimeout:
				forceSkip = true
				skipTimeout = nil
			case <-maxTimeout:
				forceSkip = true
				maxTimeout = nil
			}
			continue
		}
		forceSkip = false
		skipTimeout = nil
		maxTimeout = nil
		for _, req := range reqs {
			var err error
			if req.R.Entry == nil {
				err = p.ss.CommitEntry(req.R.EntryId, req.R.EntryXX)
			} else {
				err = p.ss.PutEntry(req.R.Entry, req.R.Committed)
			}
			req.Res.set(err)
		}
	}
}

func (p *stream) checkQLimit() error {
	if p.totalQ.Load() >= maxTotalBacklog {
		return status.Errorf(codes.Unavailable, "pipeline for: %s is fully backlogged", p.q.streamURI)
	}
	return nil
}
func (p *stream) QueueCommit(entryId int64, entryXX uint64) *ResultCtx {
	if entryId <= p.ss.TailEntryId() {
		err := p.ss.CommitEntry(entryId, entryXX)
		return newResultWithErr(err)
	}
	if err := p.checkQLimit(); err != nil {
		return newResultWithErr(err)
	}
	return p.q.Queue(&request{
		EntryId:   entryId,
		EntryXX:   entryXX,
		Committed: true,
	})
}
func (p *stream) QueuePut(e *walleapi.Entry, isCommitted bool) *ResultCtx {
	if e.EntryId <= p.ss.TailEntryId() {
		err := p.ss.PutEntry(e, isCommitted)
		return newResultWithErr(err)
	}
	if err := p.checkQLimit(); err != nil {
		return newResultWithErr(err)
	}
	return p.q.Queue(&request{
		EntryId:   e.EntryId,
		EntryXX:   e.ChecksumXX,
		Committed: isCommitted,
		Entry:     e,
	})
}
