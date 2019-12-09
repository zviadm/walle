package wallelib

import (
	"context"
	"crypto/md5"
	"sync"
	"time"

	"github.com/golang/glog"
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
	writerLease time.Duration) (*Writer, error) {
	cli, err := c.ForStream(streamURI)
	if err != nil {
		return nil, err
	}
	resp, err := cli.ClaimWriter(
		ctx, &walleapi.ClaimWriterRequest{
			StreamUri: streamURI,
			LeaseMs:   writerLease.Nanoseconds() / 1000,
		})
	if err != nil {
		return nil, err
	}
	// TODO(zviad): Lease timer should be initialzied here.
	_, err = cli.PutEntry(ctx, &walleapi.PutEntryRequest{
		StreamUri:         streamURI,
		Entry:             &walleapi.Entry{WriterId: resp.WriterId},
		CommittedEntryId:  resp.LastEntry.EntryId,
		CommittedEntryMd5: resp.LastEntry.ChecksumMd5,
	})
	if err != nil {
		return nil, err
	}
	w := newWriter(c, streamURI, writerLease, resp.WriterId, resp.LastEntry)
	return w, nil
}

// Writer is not thread safe. PutEntry calls must be issued serially.
// Writer retries PutEntry calls internally indefinitely, until an unrecoverable error happens.
// Once an error happens, writer is completely closed and can no longer be used to write any new entries.
type Writer struct {
	c           BasicClient
	streamURI   string
	writerLease time.Duration
	writerId    string
	longBeat    time.Duration

	lastEntry *walleapi.Entry

	committedEntryMx  sync.Mutex
	committedEntryId  int64
	committedEntryMd5 []byte
	commitTime        time.Time

	rootCtx    context.Context
	rootCancel context.CancelFunc
}

func newWriter(
	c BasicClient,
	streamURI string,
	writerLease time.Duration,
	writerId string,
	lastEntry *walleapi.Entry) *Writer {
	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{
		c:           c,
		streamURI:   streamURI,
		writerLease: writerLease,
		writerId:    writerId,
		longBeat:    writerLease / 10,

		lastEntry:        lastEntry,
		committedEntryId: lastEntry.EntryId,

		rootCtx:    ctx,
		rootCancel: cancel,
	}
	go w.heartbeat()
	return w
}

// Permanently closes writer and all outstanding PutEntry calls with it.
func (w *Writer) Close() {
	w.rootCancel()
	<-w.rootCtx.Done()
}

// Heartbeat makes background requests to the server if there are no active
// PutEntry calls happening. This makes sure that entries will be marked as committed fast,
// even if there are no PutEntry calls, and also makes sure that servers are aware that
// writer is still alive.
func (w *Writer) heartbeat() {
	var heartbeatTime time.Time
	var heartbeatEntryId int64
	for {
		select {
		case <-w.rootCtx.Done():
			return
		case <-time.After(shortBeat):
		}
		now := time.Now()
		ts, entryId, entryMd5 := w.safeCommittedEntryId()
		if ts.Add(shortBeat).After(now) ||
			(heartbeatEntryId == entryId && heartbeatTime.Add(w.longBeat).After(now)) {
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
					CommittedEntryId:  entryId,
					CommittedEntryMd5: entryMd5,
				})
				if err != nil && retryN > 5 {
					glog.Warningf("[%s:%d] Heartbeat: %s...", w.streamURI, entryId, err)
				}
				return err == nil, err
			})
		if err == nil {
			heartbeatEntryId = entryId
			heartbeatTime = now
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
				_, committedEntryId, committedEntryMd5 := w.safeCommittedEntryId()
				if committedEntryId > entry.EntryId {
					committedEntryId = entry.EntryId
				}
				cli, err := w.c.ForStream(w.streamURI)
				if err != nil {
					return false, err
				}
				_, err = cli.PutEntry(ctx, &walleapi.PutEntryRequest{
					StreamUri:         w.streamURI,
					Entry:             entry,
					CommittedEntryId:  committedEntryId,
					CommittedEntryMd5: committedEntryMd5,
				})
				if err == nil {
					w.updateCommittedEntryId(time.Now(), entry.EntryId, entry.ChecksumMd5)
					return true, nil
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

func (w *Writer) safeCommittedEntryId() (time.Time, int64, []byte) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	return w.commitTime, w.committedEntryId, w.committedEntryMd5
}

func (w *Writer) updateCommittedEntryId(ts time.Time, entryId int64, entryMd5 []byte) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	if ts.After(w.commitTime) {
		w.commitTime = ts
	}
	if entryId > w.committedEntryId {
		w.committedEntryId = entryId
		w.committedEntryMd5 = entryMd5
	}
}

func CalculateChecksumMd5(prevMd5 []byte, data []byte) []byte {
	h := md5.New()
	h.Write(prevMd5)
	h.Write(data)
	return h.Sum(nil)
}
