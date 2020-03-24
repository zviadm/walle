package wallelib

import (
	"context"
	"crypto/md5"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	shortBeat = time.Millisecond
)

func ClaimWriter(
	ctx context.Context,
	c BasicClient,
	streamURI string,
	writerAddr string,
	writerLease time.Duration) (*Writer, *walleapi.Entry, error) {
	cli, err := c.ForStream(streamURI)
	if err != nil {
		return nil, nil, err
	}
	resp, err := cli.ClaimWriter(
		ctx, &walleapi.ClaimWriterRequest{
			StreamUri:  streamURI,
			WriterAddr: writerAddr,
			LeaseMs:    writerLease.Nanoseconds() / time.Millisecond.Nanoseconds(),
		})
	if err != nil {
		return nil, nil, errors.Wrap(err, "")
	}
	// TODO(zviad): Lease timer should be initialzied here.
	_, err = cli.PutEntry(ctx, &walleapi.PutEntryRequest{
		StreamUri:         streamURI,
		Entry:             &walleapi.Entry{WriterId: resp.WriterId},
		CommittedEntryId:  resp.LastEntry.EntryId,
		CommittedEntryMd5: resp.LastEntry.ChecksumMd5,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "")
	}
	w := newWriter(
		c, streamURI,
		writerLease, writerAddr,
		resp.WriterId, resp.LastEntry)
	return w, resp.LastEntry, nil
}

// Writer is not thread safe. PutEntry calls must be issued serially.
// Writer retries PutEntry calls internally indefinitely, until an unrecoverable error happens.
// Once an error happens, writer is completely closed and can no longer be used to write any new entries.
type Writer struct {
	c           BasicClient
	streamURI   string
	writerLease time.Duration
	writerAddr  string
	writerId    string
	longBeat    time.Duration

	lastEntry *walleapi.Entry

	committedEntryMx  sync.Mutex
	committedEntryId  int64
	committedEntryMd5 []byte
	toCommitEntryId   int64
	toCommitEntryMd5  []byte
	commitTime        time.Time
	commitNotify      chan struct{}

	rootCtx    context.Context
	rootCancel context.CancelFunc
}

func newWriter(
	c BasicClient,
	streamURI string,
	writerLease time.Duration,
	writerAddr string,
	writerId string,
	lastEntry *walleapi.Entry) *Writer {
	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{
		c:           c,
		streamURI:   streamURI,
		writerLease: writerLease,
		writerAddr:  writerAddr,
		writerId:    writerId,
		longBeat:    writerLease / 10,

		lastEntry:         lastEntry,
		committedEntryId:  lastEntry.EntryId,
		committedEntryMd5: lastEntry.ChecksumMd5,
		toCommitEntryId:   lastEntry.EntryId,
		toCommitEntryMd5:  lastEntry.ChecksumMd5,
		commitNotify:      make(chan struct{}),

		rootCtx:    ctx,
		rootCancel: cancel,
	}
	go w.heartbeat()
	return w
}

// Permanently closes writer and all outstanding PutEntry calls with it.
// This call is thread safe and can be called at any time.
func (w *Writer) Close() {
	w.rootCancel()
	<-w.rootCtx.Done()
}

// Heartbeat makes background requests to the server if there are no active
// PutEntry calls happening. This makes sure that entries will be marked as committed fast,
// even if there are no PutEntry calls, and also makes sure that servers are aware that
// writer is still alive.
func (w *Writer) heartbeat() {
	for {
		select {
		case <-w.rootCtx.Done():
			return
		case <-time.After(shortBeat):
		}
		now := time.Now()
		ts, committedEntryId, _, toCommitEntryId, toCommitEntryMd5, _ := w.safeCommittedEntryId()
		if ts.Add(shortBeat).After(now) ||
			(committedEntryId == toCommitEntryId && ts.Add(w.longBeat).After(now)) {
			continue
		}
		err := KeepTryingWithBackoff(
			w.rootCtx, shortBeat, w.longBeat,
			func(retryN uint) (bool, error) {
				cli, err := w.c.ForStream(w.streamURI)
				if err != nil {
					return false, err
				}
				_, err = cli.PutEntry(w.rootCtx, &walleapi.PutEntryRequest{
					StreamUri:         w.streamURI,
					Entry:             &walleapi.Entry{WriterId: w.writerId},
					CommittedEntryId:  toCommitEntryId,
					CommittedEntryMd5: toCommitEntryMd5,
				})
				if err != nil {
					errStatus, _ := status.FromError(err)
					if err == context.Canceled ||
						errStatus.Code() == codes.Canceled ||
						errStatus.Code() == codes.FailedPrecondition {
						w.Close() // If there is an urecoverable error, close the writer.
						return true, err
					}
					if retryN > 5 {
						glog.Warningf(
							"[%s] Heartbeat %d->%d: %s...",
							w.streamURI, committedEntryId, toCommitEntryId, err)
					}
				}
				return err == nil, err
			})
		if err == nil {
			w.updateCommittedEntryId(now, toCommitEntryId, toCommitEntryMd5, toCommitEntryId, toCommitEntryMd5)
		}
	}
}

func (w *Writer) PutEntry(data []byte) (*walleapi.Entry, <-chan error) {
	r := make(chan error, 1)
	entry := &walleapi.Entry{
		EntryId:  w.lastEntry.EntryId + 1,
		WriterId: w.writerId,
		Data:     data,
	}
	entry.ChecksumMd5 = CalculateChecksumMd5(w.lastEntry.ChecksumMd5, data)
	w.lastEntry = entry
	go func() {
		ctx, cancel := context.WithCancel(w.rootCtx)
		defer cancel()
		err := KeepTryingWithBackoff(
			ctx, shortBeat, w.longBeat,
			func(retryN uint) (bool, error) {
				_, _, _, toCommitEntryId, toCommitEntryMd5, _ := w.safeCommittedEntryId()
				if toCommitEntryId > entry.EntryId {
					toCommitEntryId = entry.EntryId
					toCommitEntryMd5 = entry.ChecksumMd5
				}
				cli, err := w.c.ForStream(w.streamURI)
				if err != nil {
					return false, err
				}
				now := time.Now()
				_, err = cli.PutEntry(ctx, &walleapi.PutEntryRequest{
					StreamUri:         w.streamURI,
					Entry:             entry,
					CommittedEntryId:  toCommitEntryId,
					CommittedEntryMd5: toCommitEntryMd5,
				})
				if err == nil {
					w.updateCommittedEntryId(
						now, toCommitEntryId, toCommitEntryMd5, entry.EntryId, entry.ChecksumMd5)
					for {
						_, committedEntryId, _, _, _, notify := w.safeCommittedEntryId()
						if committedEntryId >= entry.EntryId {
							return true, nil
						}
						select {
						case <-ctx.Done():
							w.Close()
							return true, ctx.Err()
						case <-notify:
						}
					}
				}

				errStatus, _ := status.FromError(err)
				if err == context.Canceled ||
					errStatus.Code() == codes.Canceled ||
					errStatus.Code() == codes.FailedPrecondition {
					w.Close() // If there is an urecoverable error, close the writer.
					return true, err
				}
				if retryN > 5 {
					glog.Warningf("[%s:%d] PutEntry: %s, will retry...", w.streamURI, entry.EntryId, err)
				}
				return false, err
			})
		r <- err
	}()
	return entry, r
}

func (w *Writer) safeCommittedEntryId() (
	time.Time, int64, []byte, int64, []byte, <-chan struct{}) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	return w.commitTime, w.committedEntryId, w.committedEntryMd5, w.toCommitEntryId, w.toCommitEntryMd5, w.commitNotify
}

func (w *Writer) updateCommittedEntryId(
	ts time.Time,
	committedEntryId int64, committedEntryMd5 []byte,
	toCommitEntryId int64, toCommitEntryMd5 []byte) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	if ts.After(w.commitTime) {
		w.commitTime = ts
	}
	if committedEntryId > w.committedEntryId {
		w.committedEntryId = committedEntryId
		w.committedEntryMd5 = committedEntryMd5
		close(w.commitNotify)
		w.commitNotify = make(chan struct{})
	}
	if toCommitEntryId > w.committedEntryId {
		w.toCommitEntryId = toCommitEntryId
		w.toCommitEntryMd5 = toCommitEntryMd5
	}
}

func CalculateChecksumMd5(prevMd5 []byte, data []byte) []byte {
	h := md5.New()
	h.Write(prevMd5)
	h.Write(data)
	return h.Sum(nil)
}
