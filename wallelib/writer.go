package wallelib

import (
	"context"
	"crypto/md5"
	"sync"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	shortBeat    = time.Millisecond
	LeaseMinimum = 100 * shortBeat
)

type WriterState string

const (
	Closed       WriterState = "closed"
	Disconnected WriterState = "disconnected" // TODO(zviad): better name for this state
	Exclusive    WriterState = "exclusive"
)

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
	toCommit          *walleapi.Entry
	toCommitEntryId   int64
	toCommitEntryMd5  []byte
	commitTime        time.Time

	rootCtx    context.Context
	rootCancel context.CancelFunc
}

func newWriter(
	c BasicClient,
	streamURI string,
	writerLease time.Duration,
	writerAddr string,
	writerId string,
	lastEntry *walleapi.Entry,
	commitTime time.Time) *Writer {
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
		toCommit:          lastEntry,
		commitTime:        commitTime,

		rootCtx:    ctx,
		rootCancel: cancel,
	}
	go w.heartbeat()
	return w
}

// Permanently closes writer and all outstanding PutEntry calls with it.
// This call is thread safe and can be called at any time.
func (w *Writer) Close(tryFlush bool) {
	defer w.rootCancel()
	if !tryFlush {
		return
	}
	_, committedEntryId, _, toCommit := w.safeCommittedEntryId()
	if toCommit.EntryId <= committedEntryId {
		return
	}
	cli, err := w.c.ForStream(w.streamURI)
	if err != nil {
		return
	}
	_, _ = cli.PutEntry(w.rootCtx, &walleapi.PutEntryRequest{
		StreamUri:         w.streamURI,
		Entry:             &walleapi.Entry{WriterId: w.writerId},
		CommittedEntryId:  toCommit.EntryId,
		CommittedEntryMd5: toCommit.ChecksumMd5,
	})
}

// Returns True, if at the time of the IsWriter call, this writer is still guaranteed
// to be the only exclusive writer.
func (w *Writer) WriterState() (state WriterState, closeNotify <-chan struct{}) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	if w.rootCtx.Err() != nil {
		return Closed, w.rootCtx.Done()
	}
	if w.commitTime.Add(w.writerLease).Before(time.Now()) {
		return Disconnected, w.rootCtx.Done()
	}
	return Exclusive, w.rootCtx.Done()
}

// Returns last entry that was successfully put in the stream.
func (w *Writer) Committed() *walleapi.Entry {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	return w.toCommit
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
		ts, committedEntryId, _, toCommit := w.safeCommittedEntryId()
		if ts.Add(shortBeat).After(now) ||
			(committedEntryId == toCommit.EntryId && ts.Add(w.longBeat).After(now)) {
			continue
		}
		err := KeepTryingWithBackoff(
			w.rootCtx, w.longBeat, w.writerLease/2,
			func(retryN uint) (bool, error) {
				cli, err := w.c.ForStream(w.streamURI)
				if err != nil {
					return false, err
				}
				_, err = cli.PutEntry(w.rootCtx, &walleapi.PutEntryRequest{
					StreamUri:         w.streamURI,
					Entry:             &walleapi.Entry{WriterId: w.writerId},
					CommittedEntryId:  toCommit.EntryId,
					CommittedEntryMd5: toCommit.ChecksumMd5,
				})
				if err != nil {
					errStatus, _ := status.FromError(err)
					if err == context.Canceled ||
						errStatus.Code() == codes.Canceled ||
						errStatus.Code() == codes.FailedPrecondition {
						w.rootCancel() // If there is an urecoverable error, close the writer.
						return true, err
					}
				}
				return err == nil, err
			})
		if err != nil {
			return
		}
		w.updateCommittedEntryId(now, toCommit.EntryId, toCommit.ChecksumMd5, toCommit)
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
		// ctx, cancel := context.WithCancel(w.rootCtx)
		// defer cancel()
		err := KeepTryingWithBackoff(
			w.rootCtx, shortBeat, w.writerLease/2,
			func(retryN uint) (bool, error) {
				_, _, _, toCommit := w.safeCommittedEntryId()
				toCommitEntryId := toCommit.EntryId
				toCommitChecksumMd5 := toCommit.ChecksumMd5
				if toCommitEntryId > entry.EntryId {
					toCommitEntryId = entry.EntryId
					toCommitChecksumMd5 = entry.ChecksumMd5
				}
				cli, err := w.c.ForStream(w.streamURI)
				if err != nil {
					return false, err
				}
				now := time.Now()
				_, err = cli.PutEntry(w.rootCtx, &walleapi.PutEntryRequest{
					StreamUri:         w.streamURI,
					Entry:             entry,
					CommittedEntryId:  toCommitEntryId,
					CommittedEntryMd5: toCommitChecksumMd5,
				})
				if err == nil {
					w.updateCommittedEntryId(
						now, toCommitEntryId, toCommitChecksumMd5, entry)
					return true, nil
				}

				errStatus, _ := status.FromError(err)
				if err == context.Canceled ||
					errStatus.Code() == codes.Canceled ||
					errStatus.Code() == codes.FailedPrecondition {
					w.rootCancel() // If there is an unrecoverable error, close the writer.
					return true, err
				}
				return false, err
			})
		r <- err
	}()
	return entry, r
}

func (w *Writer) safeCommittedEntryId() (time.Time, int64, []byte, *walleapi.Entry) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	return w.commitTime, w.committedEntryId, w.committedEntryMd5, w.toCommit
}

func (w *Writer) updateCommittedEntryId(
	ts time.Time, committedEntryId int64, committedEntryMd5 []byte, toCommit *walleapi.Entry) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	if ts.After(w.commitTime) {
		w.commitTime = ts
	}
	if committedEntryId > w.committedEntryId {
		w.committedEntryId = committedEntryId
		w.committedEntryMd5 = committedEntryMd5
	}
	if toCommit.EntryId > w.toCommit.EntryId {
		w.toCommit = toCommit
	}
}

func CalculateChecksumMd5(prevMd5 []byte, data []byte) []byte {
	h := md5.New()
	h.Write(prevMd5)
	h.Write(data)
	return h.Sum(nil)
}
