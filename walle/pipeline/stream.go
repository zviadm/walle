package pipeline

import (
	"context"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stream struct {
	ss storage.Stream
	// flushQ              chan<- *ResultCtx
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
		ss: ss,
		// flushQ:              flushQ,
		fetchCommittedEntry: fetchCommittedEntry,
		q:                   newQueue(maxStreamQueueSize),
	}
	go r.process(ctx)
	return r
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
	ctx context.Context, entryId int64, entryMd5 []byte) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeoutAdjusted())
	defer cancel()
	entry, err := p.fetchCommittedEntry(
		ctx, p.ss.StreamURI(), entryId, entryMd5)
	if err != nil {
		return err
	}
	err = p.ss.PutEntry(entry, true)
	if err != nil {
		return err
	}
	zlog.Infof("[sp] stream: %s caught up to: %d (might have created a gap)", p.ss.StreamURI(), entryId)
	return nil
}

func (p *stream) process(ctx context.Context) {
	forceSkip := false
	var skipTimeout <-chan time.Time
	for ctx.Err() == nil && !p.ss.IsClosed() {
		tailId, tailNotify := p.ss.TailEntryId()
		reqs, qNotify := p.q.PopReady(tailId, forceSkip)
		if len(reqs) == 0 {
			if skipTimeout == nil && p.q.CanSkip() {
				skipTimeout = time.After(p.timeoutAdjusted())
			}
			select {
			case <-ctx.Done():
				return
			case <-qNotify:
			case <-tailNotify:
			case <-skipTimeout:
				forceSkip = true
				skipTimeout = nil
			}
			continue
		}
		forceSkip = false
		skipTimeout = nil
		for _, req := range reqs {
			var err error
			if req.R.Entry == nil {
				err = p.ss.CommitEntry(req.R.EntryId, req.R.EntryMd5)
				if err != nil && status.Convert(err).Code() == codes.OutOfRange {
					err = p.backfillEntry(ctx, req.R.EntryId, req.R.EntryMd5)
				}
			} else {
				err = p.ss.PutEntry(req.R.Entry, req.R.Committed)
			}
			req.Res.set(err)
		}
	}
}

func (p *stream) QueueCommit(entryId int64, entryMd5 []byte) *ResultCtx {
	res, ok := p.q.Queue(&Request{
		EntryId:   entryId,
		EntryMd5:  entryMd5,
		Committed: true,
	})
	if !ok {
		res = newResult()
		err := p.ss.CommitEntry(entryId, entryMd5)
		res.set(err)
	}
	return res
}
func (p *stream) QueuePut(e *walleapi.Entry, isCommitted bool) *ResultCtx {
	res, ok := p.q.Queue(&Request{
		EntryId:   e.EntryId,
		EntryMd5:  e.ChecksumMd5,
		Committed: isCommitted,
		Entry:     e,
	})
	if !ok {
		res = newResult()
		err := p.ss.PutEntry(e, isCommitted)
		res.set(err)
	}
	return res
}
