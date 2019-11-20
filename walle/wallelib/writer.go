package wallelib

import (
	"context"
	"crypto/md5"
	"sync"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Writer is not thread safe. PutEntry calls must be issued serially.
// Writer retries PutEntry calls internally indefinitely, until an unrecoverable error happens.
// Once an error happens, writer is completely closed and can no longer be used to write any new entries.
type Writer struct {
	c         walle_pb.WalleClient
	writerId  string
	lastEntry *walle_pb.Entry

	committedEntryMx sync.Mutex
	committedEntryId int64
	commitTime       time.Time

	rootCtx    context.Context
	rootCancel context.CancelFunc
}

func newWriter(
	c walle_pb.WalleClient,
	writerId string,
	lastEntry *walle_pb.Entry) *Writer {
	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{
		c:                c,
		writerId:         writerId,
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
	shortBeat := time.Millisecond
	longBeat := 100 * time.Millisecond // TODO(zviad): This should be configurable.
	for {
		select {
		case <-w.rootCtx.Done():
			return
		case <-time.After(shortBeat):
		}
		now := time.Now()
		ts, entryId := w.safeCommittedEntryId()
		if ts.Add(shortBeat).After(now) ||
			(heartbeatEntryId == entryId && heartbeatTime.Add(longBeat).After(now)) {
			continue
		}
		_, err := w.c.PutEntry(w.rootCtx, &walle_pb.PutEntryRequest{
			Entry:            &walle_pb.Entry{WriterId: w.writerId},
			CommittedEntryId: entryId,
		})
		if err == nil {
			heartbeatEntryId = entryId
			heartbeatTime = now
		}
	}
}

func (w *Writer) PutEntry(data []byte) (*walle_pb.Entry, <-chan error) {
	r := make(chan error, 1)
	entry := &walle_pb.Entry{
		EntryId:  w.lastEntry.EntryId + 1,
		WriterId: w.writerId,
		Data:     data,
	}
	entry.ChecksumMd5 = calculateChecksumMd5(w.lastEntry.ChecksumMd5, data)
	w.lastEntry = entry
	go func() {
		ctx, cancel := context.WithCancel(w.rootCtx)
		defer cancel()
		for {
			_, committedEntryId := w.safeCommittedEntryId()
			if committedEntryId > entry.EntryId {
				committedEntryId = entry.EntryId
			}
			_, err := w.c.PutEntry(ctx, &walle_pb.PutEntryRequest{
				Entry:            entry,
				CommittedEntryId: committedEntryId,
			})
			if err == nil {
				w.updateCommittedEntryId(time.Now(), entry.EntryId)
				r <- nil
				return
			}

			errStatus, _ := status.FromError(err)
			if err == context.Canceled ||
				errStatus.Code() == codes.Canceled ||
				errStatus.Code() == codes.FailedPrecondition {
				w.Close() // If there is an urecoverable error, close the writer.
				r <- err
				return
			}
			// continue retrying all other errors forever.
			// TODO(zviad): maybe add a backoff?
		}
	}()
	return entry, r
}

func (w *Writer) safeCommittedEntryId() (time.Time, int64) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	return w.commitTime, w.committedEntryId
}

func (w *Writer) updateCommittedEntryId(ts time.Time, entryId int64) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	if ts.After(w.commitTime) {
		w.commitTime = ts
	}
	if entryId > w.committedEntryId {
		w.committedEntryId = entryId
	}
}

func calculateChecksumMd5(prevMd5 []byte, data []byte) []byte {
	h := md5.New()
	h.Write(prevMd5)
	h.Write(data)
	return h.Sum(nil)
}
