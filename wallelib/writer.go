package wallelib

import (
	"context"
	"crypto/md5"
	"sync"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	shortBeat       = 5 * time.Millisecond
	LeaseMinimum    = 100 * time.Millisecond
	maxInFlightPuts = 128
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

	cliMX        sync.Mutex
	cachedCli    walleapi.WalleApiClient
	cachedCliIdx int

	lastEntry    *walleapi.Entry
	reqQ         chan *PutCtx
	inFlightQ    chan struct{}
	heartbeaterQ chan struct{}

	committedEntryMx  sync.Mutex
	committedEntryId  int64
	committedEntryMd5 []byte
	toCommit          *walleapi.Entry
	commitTime        time.Time

	rootCtx    context.Context
	rootCancel context.CancelFunc
}

type PutCtx struct {
	Entry *walleapi.Entry // Read-only!
	mx    sync.Mutex
	err   error
	done  chan struct{}
}

func (p *PutCtx) Err() error {
	p.mx.Lock()
	defer p.mx.Unlock()
	return p.err
}
func (p *PutCtx) Done() <-chan struct{} {
	return p.done
}
func (p *PutCtx) set(err error) {
	p.mx.Lock()
	defer p.mx.Unlock()
	p.err = err
	close(p.done)
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

		lastEntry: lastEntry,
		// Use very large buffer for reqQ to never block. If user fills this queue up,
		// PutEntry calls will start blocking.
		reqQ:         make(chan *PutCtx, 16384),
		inFlightQ:    make(chan struct{}, maxInFlightPuts),
		heartbeaterQ: make(chan struct{}, 1),

		committedEntryId:  lastEntry.EntryId,
		committedEntryMd5: lastEntry.ChecksumMd5,
		toCommit:          lastEntry,
		commitTime:        commitTime,

		rootCtx:    ctx,
		rootCancel: cancel,
	}
	go w.processor(ctx)
	go w.heartbeater(ctx)
	return w
}

// Permanently closes writer and all outstanding PutEntry calls with it.
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

func (w *Writer) processor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-w.reqQ:
			select {
			case <-ctx.Done():
				return
			case w.inFlightQ <- struct{}{}:
				go w.process(ctx, req)
			}
		}
	}
}

func (w *Writer) cli() (walleapi.WalleApiClient, error) {
	w.cliMX.Lock()
	defer w.cliMX.Unlock()
	if w.cachedCli != nil {
		return w.cachedCli, nil
	}
	nServers := 3 // TODO: get this from BaseClient somehow.
	for i := 0; i < nServers; i++ {
		w.cachedCliIdx += 1 % (nServers/2 + 1)
		cli, err := w.c.ForStream(w.streamURI, w.cachedCliIdx)
		if err != nil {
			if err == ErrConnUnavailable {
				continue
			}
			return nil, err
		}
		w.cachedCli = cli
		return w.cachedCli, nil
	}
	return nil, ErrConnUnavailable
}
func (w *Writer) clearCli() {
	w.cliMX.Lock()
	defer w.cliMX.Unlock()
	w.cachedCli = nil
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
		err := KeepTryingWithBackoff(
			ctx, w.longBeat, w.writerLease/4,
			func(retryN uint) (bool, bool, error) {
				if retryN >= 1 {
					w.clearCli()
				}
				cli, err := w.cli()
				if err != nil {
					return false, false, err
				}
				putCtx, cancel := context.WithTimeout(ctx, w.writerLease/4)
				defer cancel()
				_, err = cli.PutEntry(putCtx, &walleapi.PutEntryRequest{
					StreamUri:         w.streamURI,
					Entry:             &walleapi.Entry{WriterId: w.writerId},
					CommittedEntryId:  toCommit.EntryId,
					CommittedEntryMd5: toCommit.ChecksumMd5,
				})
				if err != nil {
					errStatus, _ := status.FromError(err)
					return errStatus.Code() == codes.FailedPrecondition, false, err
				}
				w.updateCommittedEntryId(now, toCommit.EntryId, toCommit.ChecksumMd5, toCommit)
				return true, false, nil
			})
		if err != nil {
			w.cancelWithErr(err)
			return
		}
	}
}

func (w *Writer) process(ctx context.Context, req *PutCtx) {
	defer func() { <-w.inFlightQ }()
	err := KeepTryingWithBackoff(
		ctx, 10*time.Millisecond, w.writerLease,
		func(retryN uint) (bool, bool, error) {
			_, _, _, toCommit := w.safeCommittedEntryId()
			if toCommit.EntryId >= req.Entry.EntryId {
				return true, false, nil
			}
			cli, err := w.cli()
			if err != nil {
				silentErr := (req.Entry.EntryId != toCommit.EntryId+1)
				return false, silentErr, err
			}
			now := time.Now()
			putCtx, cancel := context.WithTimeout(ctx, 10*time.Second) // TODO(zviad): Figure out this timeout.
			defer cancel()
			_, err = cli.PutEntry(putCtx, &walleapi.PutEntryRequest{
				StreamUri:         w.streamURI,
				Entry:             req.Entry,
				CommittedEntryId:  toCommit.EntryId,
				CommittedEntryMd5: toCommit.ChecksumMd5,
			})
			if err != nil {
				errCode := status.Convert(err).Code()
				_, _, _, toCommit := w.safeCommittedEntryId()
				silentErr := (req.Entry.EntryId <= toCommit.EntryId+1)
				return errCode == codes.FailedPrecondition, silentErr, err
			}
			w.updateCommittedEntryId(
				now, toCommit.EntryId, toCommit.ChecksumMd5, req.Entry)
			return true, false, nil
		})
	if err != nil {
		w.cancelWithErr(err)
	}
	req.set(err)
}

func (w *Writer) PutEntry(data []byte) *PutCtx {
	entry := &walleapi.Entry{
		EntryId:  w.lastEntry.EntryId + 1,
		WriterId: w.writerId,
		Data:     data,
	}
	entry.ChecksumMd5 = CalculateChecksumMd5(w.lastEntry.ChecksumMd5, data)
	w.lastEntry = entry
	r := &PutCtx{
		Entry: entry,
		done:  make(chan struct{}),
	}
	w.reqQ <- r
	return r
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
