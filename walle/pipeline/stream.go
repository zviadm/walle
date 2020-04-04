package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stream struct {
	ss                  storage.Stream
	flushQ              chan<- *ResultCtx
	fetchCommittedEntry fetchFunc

	q *queue
}

func newStream(
	ctx context.Context,
	ss storage.Stream,
	maxStreamQueueSize int,
	flushQ chan<- *ResultCtx,
	fetchCommittedEntry fetchFunc) *stream {
	r := &stream{
		ss:                  ss,
		flushQ:              flushQ,
		fetchCommittedEntry: fetchCommittedEntry,
		q:                   newQueue(maxStreamQueueSize),
	}
	go r.process(ctx)
	return r
}

func (p *stream) waitForReady(
	ctx context.Context) (int64, error) {
	waitStart := time.Now()
	maxId, tailNotify := p.ss.TailEntryId()
	for ctx.Err() == nil && !p.ss.IsClosed() {
		if p.q.IsOverflowing() {
			zlog.Warningf(
				"[sp] pipeline queue is overflowing %s: maxId: %d",
				p.ss.StreamURI(), maxId)
			return maxId, nil
		}

		head, queueNotify := p.q.Peek()
		if head == nil {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-tailNotify:
			case <-queueNotify:
			}
			waitStart = time.Now()
			continue
		}
		if head.IsReady(maxId) {
			return maxId, nil
		}
		maxId, tailNotify = p.ss.TailEntryId()
		if head.IsReady(maxId) {
			return maxId, nil
		}
		timeout := waitStart.Add(p.timeoutAdjusted()).Sub(time.Now())
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-tailNotify:
		case <-queueNotify:
		case <-time.After(timeout):
			if p.q.MaxCommittedId() > maxId {
				return maxId, nil
			}
		}
	}
	return 0, errors.Errorf("stream/pipeline is closed")
}

func (p *stream) timeoutAdjusted() time.Duration {
	timeout := time.Second
	_, _, writerLease, _ := p.ss.WriterInfo()
	if writerLease > 0 && writerLease/4 < timeout {
		timeout = writerLease / 4
	}
	return timeout
}

func (p *stream) backfillEntry(
	ctx context.Context, entryId int64, entryMd5 []byte) bool {
	ctx, cancel := context.WithTimeout(ctx, p.timeoutAdjusted())
	defer cancel()
	entry, err := p.fetchCommittedEntry(
		ctx, p.ss.StreamURI(), entryId, entryMd5)
	if err != nil {
		zlog.Warningf("[sp] err fetching: %s:%d - %s", p.ss.StreamURI(), entryId, err)
		return false
	}
	ok := p.ss.PutEntry(entry, true)
	panic.OnNotOk(ok, "committed putEntry must always succeed")
	zlog.Infof("[sp] stream: %s caught up to: %d (might have created a gap)", p.ss.StreamURI(), entryId)
	return true
}

func (p *stream) process(ctx context.Context) {
	for ctx.Err() == nil && !p.ss.IsClosed() {
		maxId, err := p.waitForReady(ctx)
		if err != nil {
			return
		}
		req := p.q.PopReady(maxId)
		var ok bool
		if req.R.Entry == nil {
			ok = p.ss.CommitEntry(req.R.EntryId, req.R.EntryMd5)
			if !ok {
				ok = p.backfillEntry(ctx, req.R.EntryId, req.R.EntryMd5)
			}
		} else {
			ok = p.ss.PutEntry(req.R.Entry, req.R.Committed)
		}
		if ok && req.R.EntryId > maxId {
			maxId = req.R.EntryId
		}
		if !ok {
			req.Res.set(status.Errorf(codes.OutOfRange, "entryId: %d", req.R.EntryId))
		} else {
			p.flushQ <- req.Res
		}
	}
}

func (p *stream) QueueCommit(entryId int64, entryMd5 []byte) *ResultCtx {
	return p.q.Queue(&Request{
		EntryId:   entryId,
		EntryMd5:  entryMd5,
		Committed: true,
	})
}
func (p *stream) QueuePut(e *walleapi.Entry, isCommitted bool) *ResultCtx {
	return p.q.Queue(&Request{
		EntryId:   e.EntryId,
		EntryMd5:  e.ChecksumMd5,
		Committed: isCommitted,
		Entry:     e,
	})
}
