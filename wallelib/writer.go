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

	tailEntry *walleapi.Entry
	reqQ      chan *PutCtx
	reqQmx    sync.Mutex // exists to coordinate writer close.

	committedEntryMx sync.Mutex
	committedEntryId int64
	committedEntryXX uint64
	toCommit         *walleapi.Entry
	commitTime       time.Time
	putsQueued       atomic.Int64

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
		longBeat:    writerLease / 4,

		tailEntry: tailEntry,
		// Use very large buffer for reqQ to never block. If user fills this queue up,
		// PutEntry calls will start blocking.
		reqQ: make(chan *PutCtx, 16384),

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
		ts, committedEntryId, _, toCommit := w.safeCommittedEntryId()
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
				w.updateCommittedEntryId(now, toCommit.EntryId, toCommit.ChecksumXX, toCommit)
				return true, false, nil
			})
		if err != nil {
			w.cancelWithErr(err)
			return
		}
	}
}

type putCtxAndErr struct {
	P *PutCtx
	E error
}

func (w *Writer) processor(ctx context.Context) {
	resultQ := make(chan putCtxAndErr, maxInFlightPuts)
	inflightWG := sync.WaitGroup{}
	var retryReqs []*PutCtx
	var nextReq *PutCtx

	defer func() {
		if ctx.Err() == nil {
			panic("Must exit only when context is done!")
		}
		go func() {
			// This can block if reqQ is full. Thus do the closing in a separate
			// go routine, if reqQ was full, rest of the cleanup will drain entries from
			// it and this lock should become available at some point.
			w.reqQmx.Lock()
			defer w.reqQmx.Unlock()
			close(w.reqQ)
		}()

		inflightWG.Wait()
		close(resultQ)
		for r := range resultQ {
			r.P.set(ctx.Err())
		}
		for _, r := range retryReqs {
			r.set(ctx.Err())
		}
		if nextReq != nil {
			nextReq.set(ctx.Err())
		}
		for req := range w.reqQ {
			req.set(ctx.Err())
		}
	}()
	tryIdx := 0
	idxByEntry := make(map[int64]int, maxInFlightPuts)
	inflightN := 0
	inflightSize := 0

	var cli ApiClient
	cliSendIdx := 0
	handleResult := func(r putCtxAndErr) {
		inflightN -= 1
		inflightSize -= r.P.Entry.Size()
		if r.E == nil {
			r.P.set(nil)
			delete(idxByEntry, r.P.Entry.EntryId)
			return
		}
		if IsErrFinal(status.Convert(r.E).Code()) {
			r.P.set(r.E)
			w.cancelWithErr(r.E)
			return
		}
		retryReqs = append(retryReqs, r.P)
		for idx := len(retryReqs) - 2; idx >= 0; idx-- {
			if retryReqs[idx].Entry.EntryId < retryReqs[idx+1].Entry.EntryId {
				break
			}
			retryReqs[idx], retryReqs[idx+1] = retryReqs[idx+1], retryReqs[idx]
		}
		if idxByEntry[r.P.Entry.EntryId] >= cliSendIdx {
			cli = nil // Reset client because it isn't successful.
		}
	}
	for ; ctx.Err() == nil; tryIdx++ {
		if cli == nil || !cli.IsPreferred() {
			// First make sure we have connected client to make requests with.
			var err error
			cli, err = w.c.ForStream(w.streamURI)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case res := <-resultQ:
					handleResult(res)
				case <-time.After(w.longBeat):
				}
				continue
			}
			cliSendIdx = tryIdx
		}

		if nextReq == nil && len(retryReqs) == 0 {
			select {
			case <-ctx.Done():
				return
			case nextReq = <-w.reqQ:
			case res := <-resultQ:
				handleResult(res)
			}
			continue
		}
		var req *PutCtx
		if len(retryReqs) > 0 {
			retryReq := retryReqs[0]
			prevIdx, ok := idxByEntry[retryReq.Entry.EntryId-1]
			if ok && prevIdx < idxByEntry[retryReq.Entry.EntryId] {
				select {
				case <-ctx.Done():
					return
				case res := <-resultQ:
					handleResult(res)
				}
				continue
			}
			req = retryReq
			retryReqs[0] = nil
			retryReqs = retryReqs[1:]
		} else {
			// check inflight requirements.
			if inflightN+1 > maxInFlightPuts ||
				inflightSize+nextReq.Entry.Size() > maxInFlightSize {
				select {
				case <-ctx.Done():
					return
				case res := <-resultQ:
					handleResult(res)
				}
				continue
			}
			req, nextReq = nextReq, nil
		}

		idxByEntry[req.Entry.EntryId] = tryIdx
		inflightWG.Add(1)
		inflightN += 1
		inflightSize += req.Entry.Size()
		go func(cli walleapi.WalleApiClient, req *PutCtx) {
			err := w.putEntry(ctx, cli, req.Entry)
			resultQ <- putCtxAndErr{P: req, E: err}
			inflightWG.Done()
		}(cli, req)
	}
}

func (w *Writer) putEntry(ctx context.Context, cli walleapi.WalleApiClient, entry *walleapi.Entry) error {
	timeout := putEntryTimeout
	if w.writerLease > timeout {
		timeout = w.writerLease
	}
	_, _, _, toCommit := w.safeCommittedEntryId()
	if toCommit.EntryId >= entry.EntryId {
		return nil
	}
	putCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	now := time.Now()
	_, err := cli.PutEntry(putCtx, &walleapi.PutEntryRequest{
		StreamUri:        w.streamURI,
		Entry:            entry,
		CommittedEntryId: toCommit.EntryId,
		CommittedEntryXX: toCommit.ChecksumXX,
	})
	if err == nil {
		w.putsQueued.Add(-1)
		w.updateCommittedEntryId(
			now, toCommit.EntryId, toCommit.ChecksumXX, entry)
	}
	return err
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
	w.reqQmx.Lock()
	defer w.reqQmx.Unlock()
	if err := w.rootCtx.Err(); err != nil {
		r.set(err)
	} else {
		w.putsQueued.Add(1)
		w.reqQ <- r
	}
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
