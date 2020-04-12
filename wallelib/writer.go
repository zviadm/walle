package wallelib

import (
	"context"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/status"
)

const (
	// LeaseMinimum is absolute minimum that can be provided as writer lease duration.
	// More realistic range of writer lease would be ~1-10seconds.
	LeaseMinimum = 100 * time.Millisecond

	// MaxEntrySize is maximum entry size in WALLE. This can't be changed.
	MaxEntrySize = 1024 * 1024 // 1MB

	maxInFlightSize = 4 * 1024 * 1024 // Maximum put request bytes in-flight.
	maxInFlightPuts = 128             // Maximum number of put requests in-flight.

	// shortBeat duration defines minimum amount of time it takes for explicit Commit
	// to be called after a successful PutEntry request, if there are no other PutEntry
	// requests made. Explicit Commits are avoided if PutEntry traffic is fast enough
	// since PutEntry calls themselves can also act as explicit Commit calls for previous entries.
	shortBeat = 5 * time.Millisecond

	connectTimeout  = time.Second
	putEntryTimeout = 5 * time.Second
	watchTimeout    = 5 * time.Second
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

	cliMX     sync.Mutex
	cachedCli ApiClient

	tailEntry *walleapi.Entry
	reqQ      chan *PutCtx
	limiter   *limiter

	committedEntryMx sync.Mutex
	committedEntryId int64
	committedEntryXX uint64
	toCommit         *walleapi.Entry
	commitTime       time.Time

	rootCtx    context.Context
	rootCancel context.CancelFunc
}

// PutCtx provides context.Context like interface for PutEntry results.
type PutCtx struct {
	Entry *walleapi.Entry // Read-only!
	mx    sync.Mutex
	err   error
	done  chan struct{}
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
		longBeat:    writerLease / 8,

		tailEntry: tailEntry,
		// Use very large buffer for reqQ to never block. If user fills this queue up,
		// PutEntry calls will start blocking.
		reqQ:    make(chan *PutCtx, 16384),
		limiter: newLimiter(maxInFlightPuts, maxInFlightSize),

		committedEntryId: tailEntry.EntryId,
		committedEntryXX: tailEntry.ChecksumXX,
		toCommit:         tailEntry,
		commitTime:       commitTime,

		rootCtx:    ctx,
		rootCancel: cancel,
	}
	go w.processor(ctx)
	go w.heartbeater(ctx)
	return w
}

// Close permanently closes writer and all outstanding PutEntry calls with it.
// This call is thread safe and can be called at any time.
func (w *Writer) Close() {
	w.rootCancel()
}

func (w *Writer) cancelWithErr(err error) {
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

func (w *Writer) processor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-w.reqQ:
			for {
				ok, notify := w.limiter.Put(req.Entry.Size())
				if ok {
					go w.process(ctx, req)
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-notify:
				}
			}
		}
	}
}

func (w *Writer) cli(cliWithErr ApiClient) (ApiClient, error) {
	w.cliMX.Lock()
	defer w.cliMX.Unlock()
	var err error
	if w.cachedCli == nil || w.cachedCli == cliWithErr || !w.cachedCli.IsPreferred() {
		w.cachedCli, err = w.c.ForStream(w.streamURI)
	}
	return w.cachedCli, err
}

// Heartbeater makes requests to the server if there are no active PutEntry calls happening.
// This makes sure that entries will be marked as committed fast, even if there are no PutEntry calls,
// and also makes sure that servers are aware that writer is still alive.
func (w *Writer) heartbeater(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(shortBeat):
		}
		now := time.Now()
		ts, committedEntryId, _, toCommit := w.safeCommittedEntryId()
		if ts.Add(shortBeat).After(now) ||
			(committedEntryId == toCommit.EntryId && ts.Add(w.longBeat).After(now)) {
			continue
		}
		var cli ApiClient
		err := KeepTryingWithBackoff(
			ctx, w.longBeat, w.writerLease/4,
			func(retryN uint) (bool, bool, error) {
				var err error
				cli, err = w.cli(cli)
				if err != nil {
					return false, false, err
				}
				putCtx, cancel := context.WithTimeout(ctx, w.writerLease/4)
				defer cancel()
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
				w.updateCommittedEntryId(now, toCommit.EntryId, toCommit.ChecksumXX, toCommit)
				return true, false, nil
			})
		if err != nil {
			w.cancelWithErr(err)
			return
		}
	}
}

func (w *Writer) process(ctx context.Context, req *PutCtx) {
	defer w.limiter.Done(req.Entry.Size())
	timeout := putEntryTimeout
	if w.writerLease > timeout {
		timeout = w.writerLease
	}
	var cli ApiClient
	err := KeepTryingWithBackoff(
		ctx, w.longBeat, w.writerLease,
		func(retryN uint) (bool, bool, error) {
			_, _, _, toCommit := w.safeCommittedEntryId()
			if toCommit.EntryId >= req.Entry.EntryId {
				return true, false, nil
			}
			var err error
			cli, err = w.cli(cli)
			if err != nil {
				silentErr := (req.Entry.EntryId > toCommit.EntryId+1)
				return false, silentErr, err
			}
			now := time.Now()
			putCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			_, err = cli.PutEntry(putCtx, &walleapi.PutEntryRequest{
				StreamUri:        w.streamURI,
				Entry:            req.Entry,
				CommittedEntryId: toCommit.EntryId,
				CommittedEntryXX: toCommit.ChecksumXX,
			})
			if err != nil {
				_, _, _, toCommit := w.safeCommittedEntryId()
				if req.Entry.EntryId <= toCommit.EntryId {
					return true, false, nil
				}
				silentErr := (req.Entry.EntryId > toCommit.EntryId+1)
				errCode := status.Convert(err).Code()
				return IsErrFinal(errCode), silentErr, err
			}
			w.updateCommittedEntryId(
				now, toCommit.EntryId, toCommit.ChecksumXX, req.Entry)
			return true, false, nil
		})
	if err != nil {
		w.cancelWithErr(err)
	}
	req.set(err)
}

// PutEntry puts new entry in the stream.
func (w *Writer) PutEntry(data []byte) *PutCtx {
	entry := &walleapi.Entry{
		EntryId:  w.tailEntry.EntryId + 1,
		WriterId: w.writerId,
		Data:     data,
	}
	entry.ChecksumXX = CalculateChecksumXX(w.tailEntry.ChecksumXX, data)
	w.tailEntry = entry
	r := &PutCtx{
		Entry: entry,
		done:  make(chan struct{}),
	}
	w.reqQ <- r
	return r
}

func (w *Writer) safeCommittedEntryId() (time.Time, int64, uint64, *walleapi.Entry) {
	w.committedEntryMx.Lock()
	defer w.committedEntryMx.Unlock()
	return w.commitTime, w.committedEntryId, w.committedEntryXX, w.toCommit
}

func (w *Writer) updateCommittedEntryId(
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

// Limiter for maximum number and size of requests that are in-flight.
type limiter struct {
	maxN    int
	maxSize int

	mx           sync.Mutex
	inflightN    int
	inflightSize int
	notify       chan struct{}
}

func newLimiter(maxN int, maxSize int) *limiter {
	return &limiter{
		maxN:    maxN,
		maxSize: maxSize,
		notify:  make(chan struct{}),
	}
}

func (l *limiter) Put(size int) (bool, <-chan struct{}) {
	l.mx.Lock()
	defer l.mx.Unlock()
	ok := (l.inflightN+1 <= l.maxN) && (l.inflightSize+size <= l.maxSize)
	if ok {
		l.inflightN += 1
		l.inflightSize += size
	}
	return ok, l.notify
}

func (l *limiter) Done(size int) {
	l.mx.Lock()
	defer l.mx.Unlock()
	l.inflightN -= 1
	l.inflightSize -= size
	close(l.notify)
	l.notify = make(chan struct{})
}
