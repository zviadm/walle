package wallelib

import (
	"context"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/zlog"
	"go.uber.org/atomic"
	"google.golang.org/grpc/status"
)

const (
	// LeaseMinimum is absolute minimum that can be provided as writer lease duration.
	// More realistic range of writer lease would be ~[1-10] seconds.
	LeaseMinimum = 100 * time.Millisecond

	// MaxEntrySize is maximum entry size in WALLE. This can't be changed.
	MaxEntrySize = 1024 * 1024 // 1MB

	maxInFlightSize = 4 * 1024 * 1024 // Maximum put requests in-flight as bytes.
	maxInFlightPuts = 128             // Maximum number of put requests in-flight.

	// shortBeat duration defines minimum amount of time it takes for explicit Commit
	// to be called after a successful PutEntry request, if there are no other PutEntry
	// requests made. Explicit Commits are avoided if PutEntry traffic is fast enough
	// since PutEntry calls themselves can also act as explicit Commit calls for previous entries.
	shortBeat = 5 * time.Millisecond

	connectTimeout  = time.Second
	putEntryTimeout = 5 * time.Second
	pollTimeout     = 5 * time.Second
	// ReconnectDelay is maximum backoff timeout for establishing connections.
	ReconnectDelay = 5 * time.Second
)

// Writer is not thread safe. PutEntry calls must be issued serially.
// Writer retries PutEntry calls internally indefinitely, until an unrecoverable error happens.
// Once an error happens, writer is completely closed and can no longer be used to write any new entries.
type Writer struct {
	c           Client
	streamURI   string
	writerLease time.Duration
	writerAddr  string
	writerId    []byte
	longBeat    time.Duration

	p         *processor
	tailEntry *walleapi.Entry

	committedEntryMx sync.Mutex
	committedEntryId int64
	committedEntryXX uint64
	toCommit         *walleapi.Entry
	commitTime       time.Time
	putsQueued       atomic.Int64

	rootCtx    context.Context
	rootCancel context.CancelFunc
}

func newWriter(
	c Client,
	streamURI string,
	writerLease time.Duration,
	writerAddr string,
	writerId []byte,
	tailEntry *walleapi.Entry,
	commitTime time.Time) *Writer {
	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{
		c:           c,
		streamURI:   streamURI,
		writerLease: writerLease,
		writerAddr:  writerAddr,
		writerId:    writerId,
		longBeat:    writerLease / 4,

		tailEntry: tailEntry,

		committedEntryId: tailEntry.EntryId,
		committedEntryXX: tailEntry.ChecksumXX,
		toCommit:         tailEntry,
		commitTime:       commitTime,

		rootCtx:    ctx,
		rootCancel: cancel,
	}
	w.p = newProcessor(
		ctx, w.cancelCtx, w.newPutter, maxInFlightPuts, maxInFlightSize)
	go w.heartbeater(ctx)
	return w
}

// Close permanently closes writer and all outstanding PutEntry calls with it.
// This call is thread safe and can be called at any time.
func (w *Writer) Close() {
	w.rootCancel()
}

func (w *Writer) cancelCtx(err error) {
	if w.rootCtx.Err() != nil {
		return
	}
	zlog.Warningf("writer:%s closed due to unrecoverable err: %s", w.writerAddr, err)
	w.Close()
}

// Done returns channel that will get closed when Writer itself becomes closed.
// Writer will automatically close if it determines that it is no longer the
// exclusive writer and no further PutEntry calls can succeed.
func (w *Writer) Done() <-chan struct{} {
	return w.rootCtx.Done()
}

// IsExclusive returns True, if at the time of the IsExclusive() call, it is guaranteed that
// there was no other writer that could have written any entries. Note that Writer can be closed,
// but still exclusive for some time period, due to how leasing works.
func (w *Writer) IsExclusive() bool {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	return w.commitTime.Add(w.writerLease * 3 / 4).After(time.Now())
}

// Committed returns last entry that was successfully put in the stream.
func (w *Writer) Committed() *walleapi.Entry {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	return w.toCommit
}

// Heartbeater makes requests to the server if there are no active PutEntry calls happening.
// This makes sure that entries will be marked as committed fast, even if there are no PutEntry calls,
// and also makes sure that servers are aware that writer is still alive.
func (w *Writer) heartbeater(ctx context.Context) {
	var cli ApiClient
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(shortBeat):
		}
		now := time.Now()
		ts, committedEntryId, _, toCommit := w.commitInfo()
		if ts.Add(w.longBeat).After(now) &&
			(committedEntryId == toCommit.EntryId || w.putsQueued.Load() > 0) {
			// Skip heartbeat if:
			//   - Long beat hasn't been missed
			//   - Either regular puts are already queued, or there is nothing new to commit.
			continue
		}
		err := KeepTryingWithBackoff(
			ctx, w.longBeat/4, w.longBeat,
			func(retryN uint) (bool, bool, error) {
				var err error
				if cli == nil || !cli.IsPreferred() || retryN > 0 {
					cli, err = w.c.ForStream(w.streamURI)
				}
				if err != nil {
					return false, false, err
				}
				putCtx, cancel := context.WithTimeout(ctx, w.longBeat)
				defer cancel()
				now := time.Now()
				_, err = cli.PutEntry(putCtx, &walleapi.PutEntryRequest{
					StreamUri:        w.streamURI,
					Entry:            &walleapi.Entry{WriterId: w.writerId},
					CommittedEntryId: toCommit.EntryId,
					CommittedEntryXX: toCommit.ChecksumXX,
				})
				if err != nil {
					errCode := status.Convert(err).Code()
					return IsErrFinal(errCode), false, err
				}
				w.updateCommitInfo(now, toCommit.EntryId, toCommit.ChecksumXX, toCommit)
				return true, false, nil
			})
		if err != nil {
			w.cancelCtx(err)
			return
		}
	}
}

func (w *Writer) newPutter() (entryPutter, error) {
	c, err := w.c.ForStream(w.streamURI)
	if err != nil {
		return nil, err
	}
	return wPutter{ApiClient: c, w: w}, nil
}

type wPutter struct {
	ApiClient
	w *Writer
}

// Put implements entryPutter interface and makes actual RPC call to WALLE server.
func (p wPutter) Put(ctx context.Context, entry *walleapi.Entry) error {
	timeout := putEntryTimeout
	if p.w.writerLease > timeout {
		timeout = p.w.writerLease
	}
	_, _, _, toCommit := p.w.commitInfo()
	if toCommit.EntryId >= entry.EntryId {
		return nil
	}
	putCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	now := time.Now()
	_, err := p.PutEntry(putCtx, &walleapi.PutEntryRequest{
		StreamUri:        p.w.streamURI,
		Entry:            entry,
		CommittedEntryId: toCommit.EntryId,
		CommittedEntryXX: toCommit.ChecksumXX,
	})
	if err == nil {
		p.w.putsQueued.Add(-1)
		p.w.updateCommitInfo(
			now, toCommit.EntryId, toCommit.ChecksumXX, entry)
	}
	return err
}

// commitInfo returns information about last successful commit and also
// last successful put that may not have been explicitly committed yet.
func (w *Writer) commitInfo() (time.Time, int64, uint64, *walleapi.Entry) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	return w.commitTime, w.committedEntryId, w.committedEntryXX, w.toCommit
}

// updateCommitInfo updates information about last successful commit and
// also last successful put.
func (w *Writer) updateCommitInfo(
	ts time.Time, committedEntryId int64, committedEntryXX uint64, toCommit *walleapi.Entry) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	if ts.After(w.commitTime) {
		w.commitTime = ts
	}
	if committedEntryId > w.committedEntryId {
		w.committedEntryId = committedEntryId
		w.committedEntryXX = committedEntryXX
	}
	if toCommit.EntryId > w.toCommit.EntryId {
		w.toCommit = toCommit
	}
}

// PutEntry puts new entry in the stream. PutEntry call itself doesn't block.
// This call is NOT thread-safe.
func (w *Writer) PutEntry(data []byte) *PutCtx {
	r := makePutCtx(w.tailEntry, w.writerId, data)
	w.tailEntry = r.Entry
	w.putsQueued.Add(1)
	w.p.Queue(r)
	return r
}

// PutCtx provides context.Context like interface for PutEntry results.
type PutCtx struct {
	Entry *walleapi.Entry // Read-only!
	mx    sync.Mutex
	err   error
	done  chan struct{}
}

func makePutCtx(prevEntry *walleapi.Entry, writerId []byte, data []byte) *PutCtx {
	entry := &walleapi.Entry{
		EntryId:  prevEntry.EntryId + 1,
		WriterId: writerId,
		Data:     data,
	}
	entry.ChecksumXX = CalculateChecksumXX(prevEntry.ChecksumXX, data)
	return &PutCtx{
		Entry: entry,
		done:  make(chan struct{}),
	}
}

func (p *PutCtx) set(err error) {
	p.mx.Lock()
	defer p.mx.Unlock()
	p.err = err
	close(p.done)
}

// Err returns result of PutCtx. Value of Err() is relevant only after
// Done() channel is closed.
func (p *PutCtx) Err() error {
	p.mx.Lock()
	defer p.mx.Unlock()
	return p.err
}

// Done returns channel that will be closed once Err() is set.
func (p *PutCtx) Done() <-chan struct{} {
	return p.done
}

// CalculateChecksumXX calculates checksum of new entry given checksum
// of the previous entry.
func CalculateChecksumXX(prevXX uint64, data []byte) uint64 {
	h := xxhash.New()
	b := []byte{
		byte(prevXX),
		byte(prevXX >> 8),
		byte(prevXX >> 16),
		byte(prevXX >> 24),
		byte(prevXX >> 32),
		byte(prevXX >> 40),
		byte(prevXX >> 48),
		byte(prevXX >> 56),
	}
	h.Write(b)
	h.Write(data)
	return h.Sum64()
}
