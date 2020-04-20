package storage

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/zviadm/stats-go/metrics"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/wt"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// streamStorage keeps 8 bytes of ChecksumXX for each uncommitted entry. This leads
	// to 32KB of memory use per stream. This is pretty small compared to all other buffers and
	// caches that exist in the stream for each stream.
	maxUncommittedEntries = 4 * 1024
)

var (
	streamCfg = wt.DataSourceCfg{
		BlockCompressor:   "snappy",
		AccessPatternHint: wt.AccessSequential,
		// Configure LeafPageMax large enough to get most of the benefit of sequential
		// data transfer. On SSDs, configuring block size larger than 256KB would bring
		// only very marginal benefits, at cost of much larger page sizes.
		LeafPageMax: 256 * 1024,
		// Make sure no values ever overflow, and all values are stored in leaf pages,
		// even if it means that leaf pages will end up being larger for those items.
		LeafValueMax: 2 * 1024 * 1024,
		// Since all writes are sequential, page split should result in creating
		// new full sized page.
		SplitPct: 100,
		// Keep MemoryPageMax relatively small to make sure no insert blocks for too
		// long. It is better to have more frequent smaller stalls for WALLE.
		MemoryPageMax: 4 * 1024 * 1024,
	}
)

type streamStorage struct {
	serverId  string
	streamURI string

	// Separate read only session and lock for ReadFrom calls.
	roMX   sync.Mutex
	sessRO *wt.Session
	roBuf8 []byte

	// Separate session for writing Gap entries.
	backfillMX       sync.Mutex
	sessFill         *wt.Session
	streamFillC      *wt.Cursor
	backfillBuf8     []byte
	backfillEntryBuf []byte

	// mx protects all non-atomic variables below. and also protects
	// WT sess and all its cursors.
	mx       sync.Mutex
	sess     *wt.Session
	metaC    *wt.Cursor
	streamC  *wt.Cursor
	buf8     []byte
	entryBuf []byte
	topology atomic.Value // Type is: *walleapi.StreamTopology

	wInfo        *writerInfo
	wInfoRO      atomic.Value // *writerInfo
	renewedLease atomic.Value // time.Time
	gapStartId   atomic.Int64
	gapEndId     atomic.Int64
	committed    atomic.Int64
	commitNotify chan struct{}

	tailEntry   *walleapi.Entry
	tailEntryId atomic.Int64
	// tailEntryXX is a circular buffer that contains entry.ChecksumXX for entries:
	// [committed..tailEntryId]. EntryId maps to index: EntryId%(len(tailEntryXX)).
	tailEntryXX [maxUncommittedEntries]uint64

	// metadata keys
	mkWriterId      []byte
	mkWriterAddr    []byte
	mkWriterLeaseNs []byte
	mkCommittedId   []byte
	mkGapStartId    []byte
	mkGapEndId      []byte

	// stats
	committedIdG metrics.Gauge
	gapStartIdG  metrics.Gauge
	gapEndIdG    metrics.Gauge
	tailIdG      metrics.Gauge

	backfillsC       metrics.Counter
	backfillBytesC   metrics.Counter
	backfillTotalMsC metrics.Counter

	cursorsG     metrics.Gauge
	cursorNextsC metrics.Counter
}

type writerInfo struct {
	Id    walleapi.WriterId
	Addr  string
	Lease time.Duration
}

func createStreamStorage(
	serverId string,
	streamURI string,
	sess *wt.Session,
	sessRO *wt.Session,
	sessFill *wt.Session) Stream {
	panic.OnErr(ValidateStreamURI(streamURI))
	panic.OnErr(sess.Create(streamDS(streamURI), streamCfg))
	panic.OnErr(sess.Create(streamBackfillDS(streamURI), streamCfg))

	panic.OnErr(sess.TxBegin(wt.TxCfg{Sync: wt.True}))
	metaC, err := sess.OpenCursor(metadataDS)
	panic.OnErr(err)
	streamC, err := sess.OpenCursor(streamDS(streamURI))
	panic.OnErr(err)

	writerId0, err := Entry0.WriterId.Marshal()
	panic.OnErr(err)
	panic.OnErr(metaC.Insert([]byte(streamURI+sfxWriterId), writerId0))
	panic.OnErr(metaC.Insert([]byte(streamURI+sfxWriterAddr), []byte{}))
	panic.OnErr(metaC.Insert([]byte(streamURI+sfxWriterLeaseNs), make([]byte, 8)))
	panic.OnErr(metaC.Insert([]byte(streamURI+sfxCommittedId), make([]byte, 8)))
	panic.OnErr(metaC.Insert([]byte(streamURI+sfxGapStartId), make([]byte, 8)))
	panic.OnErr(metaC.Insert([]byte(streamURI+sfxGapEndId), make([]byte, 8)))
	panic.OnErr(streamC.Insert(make([]byte, 8), entry0B))
	panic.OnErr(sess.TxCommit())
	panic.OnErr(metaC.Close())
	panic.OnErr(streamC.Close())
	return openStreamStorage(serverId, streamURI, sess, sessRO, sessFill)
}

func openStreamStorage(
	serverId string,
	streamURI string,
	sess *wt.Session,
	sessRO *wt.Session,
	sessFill *wt.Session) Stream {
	metricsKV := metrics.KV{"stream_uri": streamURI}
	r := &streamStorage{
		serverId:  serverId,
		streamURI: streamURI,

		sess:     sess,
		buf8:     make([]byte, 8),
		entryBuf: make([]byte, entryMaxSerializedSize),

		sessRO: sessRO,
		roBuf8: make([]byte, 8),

		sessFill:         sessFill,
		backfillBuf8:     make([]byte, 8),
		backfillEntryBuf: make([]byte, entryMaxSerializedSize),

		// metadata Keys
		mkWriterId:      []byte(streamURI + sfxWriterId),
		mkWriterAddr:    []byte(streamURI + sfxWriterAddr),
		mkWriterLeaseNs: []byte(streamURI + sfxWriterLeaseNs),
		mkCommittedId:   []byte(streamURI + sfxCommittedId),
		mkGapStartId:    []byte(streamURI + sfxGapStartId),
		mkGapEndId:      []byte(streamURI + sfxGapEndId),

		// stats
		committedIdG: committedIdGauge.V(metricsKV),
		gapStartIdG:  gapStartIdGauge.V(metricsKV),
		gapEndIdG:    gapEndIdGauge.V(metricsKV),
		tailIdG:      tailIdGauge.V(metricsKV),

		backfillsC:       backfillsCounter.V(metricsKV),
		backfillBytesC:   backfillBytesCounter.V(metricsKV),
		backfillTotalMsC: backfillTotalMsCounter.V(metricsKV),

		cursorsG:     streamCursorsGauge.V(metricsKV),
		cursorNextsC: cursorNextsCounter.V(metricsKV),
	}
	var err error
	r.metaC, err = sess.OpenCursor(metadataDS)
	panic.OnErr(err)
	r.streamC, err = sess.OpenCursor(streamDS(streamURI))
	panic.OnErr(err)
	r.streamFillC, err = sessFill.OpenCursor(streamBackfillDS(streamURI))
	panic.OnErr(err)

	r.wInfo = &writerInfo{}
	v, err := r.metaC.ReadValue(r.mkWriterId)
	panic.OnErr(err)
	err = r.wInfo.Id.Unmarshal(v)
	panic.OnErr(err)
	v, err = r.metaC.ReadValue(r.mkWriterAddr)
	panic.OnErr(err)
	r.wInfo.Addr = string(v)
	v, err = r.metaC.ReadValue(r.mkWriterLeaseNs)
	panic.OnErr(err)
	r.wInfo.Lease = time.Duration(binary.BigEndian.Uint64(v))
	r.wInfoRO.Store(r.wInfo)
	r.renewedLease.Store(time.Time{})

	v, err = r.metaC.ReadValue(r.mkCommittedId)
	panic.OnErr(err)
	committed := int64(binary.BigEndian.Uint64(v))
	r.committed.Store(committed)
	r.committedIdG.Set(float64(committed))
	v, err = r.metaC.ReadValue(r.mkGapStartId)
	panic.OnErr(err)
	gapStartId := int64(binary.BigEndian.Uint64(v))
	r.gapStartId.Store(gapStartId)
	r.gapStartIdG.Set(float64(gapStartId))
	v, err = r.metaC.ReadValue(r.mkGapEndId)
	panic.OnErr(err)
	gapEndId := int64(binary.BigEndian.Uint64(v))
	r.gapEndId.Store(gapEndId)
	r.gapEndIdG.Set(float64(gapEndId))

	// Read all tail entries from storage to populate: r.tailEntryXX array.
	binary.BigEndian.PutUint64(r.buf8, uint64(committed))
	err = r.streamC.Search(r.buf8)
	panic.OnErr(err)
	var tailEntry *walleapi.Entry
	for {
		tailEntry = unmarshalValue(r.streamURI, committed, r.streamC)
		r.tailEntryXX[tailEntry.EntryId%int64(len(r.tailEntryXX))] = tailEntry.ChecksumXX
		err := r.streamC.Next()
		if err != nil && wt.ErrCode(err) == wt.ErrNotFound {
			break
		}
		panic.OnErr(err)
	}
	r.updateTailEntry(tailEntry)
	panic.OnErr(r.metaC.Reset())
	panic.OnErr(r.streamC.Reset())
	return r
}

func (m *streamStorage) StreamURI() string {
	return m.streamURI
}

func (m *streamStorage) close() {
	m.roMX.Lock()
	panic.OnErr(m.sessRO.Close())
	m.cursorsG.Set(0)
	m.roMX.Unlock()

	m.backfillMX.Lock()
	panic.OnErr(m.sessFill.Close())
	m.backfillMX.Unlock()

	m.mx.Lock()
	defer m.mx.Unlock()
	panic.OnErr(m.sess.Close())
	if m.commitNotify != nil {
		close(m.commitNotify)
	}
	m.committedIdG.Set(0)
	m.gapStartIdG.Set(0)
	m.gapEndIdG.Set(0)
	m.tailIdG.Set(0)
}
func (m *streamStorage) IsClosed() bool {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.sess.Closed()
}

func (m *streamStorage) Topology() *walleapi.StreamTopology {
	return m.topology.Load().(*walleapi.StreamTopology)
}
func (m *streamStorage) setTopology(t *walleapi.StreamTopology) {
	m.topology.Store(t)
}

func (m *streamStorage) WriterInfo() (walleapi.WriterId, string, time.Duration, time.Duration) {
	wInfo := m.wInfoRO.Load().(*writerInfo)
	remainingLease := m.renewedLease.Load().(time.Time).Add(wInfo.Lease).Sub(time.Now())
	return wInfo.Id, wInfo.Addr, wInfo.Lease, remainingLease
}
func (m *streamStorage) UpdateWriter(
	writerId walleapi.WriterId, writerAddr string, lease time.Duration) (time.Duration, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return 0, status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}
	cmpWriterId := CmpWriterIds(writerId, m.wInfo.Id)
	if cmpWriterId < 0 {
		return 0, status.Errorf(codes.FailedPrecondition, "%v < %v", writerId, m.wInfo.Id)
	}
	if cmpWriterId == 0 {
		return 0, nil
	}
	now := time.Now()
	remainingLease := m.renewedLease.Load().(time.Time).Add(m.wInfo.Lease).Sub(now)
	m.wInfo = &writerInfo{
		Id:    writerId,
		Addr:  writerAddr,
		Lease: lease,
	}
	m.wInfoRO.Store(m.wInfo)
	m.renewedLease.Store(now)

	writerIdB, err := m.wInfo.Id.Marshal()
	panic.OnErr(err)
	panic.OnErr(m.sess.TxBegin(wt.TxCfg{Sync: wt.True}))
	panic.OnErr(m.metaC.Insert(m.mkWriterId, writerIdB))
	panic.OnErr(m.metaC.Insert(m.mkWriterAddr, []byte(m.wInfo.Addr)))
	binary.BigEndian.PutUint64(m.buf8, uint64(m.wInfo.Lease.Nanoseconds()))
	panic.OnErr(m.metaC.Insert(m.mkWriterLeaseNs, []byte(m.buf8)))
	panic.OnErr(m.sess.TxCommit())
	return remainingLease, nil
}
func (m *streamStorage) RenewLease(
	writerId walleapi.WriterId, extraBuffer time.Duration) error {
	// It is always safe to update renewLease, however we must never
	// return success if writer has changed.
	panic.OnNotOk(extraBuffer >= 0, "extra buffer must be >=0: %s", extraBuffer)
	m.renewedLease.Store(time.Now().Add(extraBuffer))
	mWriterId := m.wInfoRO.Load().(*writerInfo).Id
	if CmpWriterIds(mWriterId, writerId) != 0 {
		return status.Errorf(codes.FailedPrecondition, "%v != %v", writerId, mWriterId)
	}
	return nil
}

func (m *streamStorage) TailEntries(n int) ([]*walleapi.Entry, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return nil, status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}
	committed := m.committed.Load()
	binary.BigEndian.PutUint64(m.buf8, uint64(committed))
	mType, err := m.streamC.SearchNear(m.buf8)
	panic.OnErr(err)
	panic.OnNotOk(mType == wt.MatchedExact, "committed entries mustn't have any gaps")

	tailN := int(m.tailEntry.EntryId - committed + 1)
	if n > 0 && n < tailN {
		tailN = n
	}
	r := make([]*walleapi.Entry, tailN)
	for idx := range r {
		r[idx] = unmarshalValue(m.streamURI, committed, m.streamC)
		panic.OnNotOk(
			r[idx].EntryId == committed+int64(idx),
			"tail entry missing: %s [%d..%d] %d",
			m.streamURI, m.committed, m.tailEntry.EntryId, idx)
		if idx != len(r)-1 {
			panic.OnErr(m.streamC.Next())
		} else {
			panic.OnErr(m.streamC.Reset())
		}
	}
	return r, nil
}

func (m *streamStorage) CommitNotify() <-chan struct{} {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.commitNotify == nil {
		m.commitNotify = make(chan struct{})
	}
	return m.commitNotify
}
func (m *streamStorage) CommittedId() int64 {
	return m.committed.Load()
}
func (m *streamStorage) TailEntryId() int64 {
	return m.tailEntryId.Load()
}
func (m *streamStorage) GapRange() (startId int64, endId int64) {
	return m.gapStartId.Load(), m.gapEndId.Load()
}
func (m *streamStorage) UpdateGapStart(entryId int64) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return
	}
	gapStartId := m.gapStartId.Load()
	if gapStartId == 0 || entryId <= gapStartId {
		return
	}
	if entryId >= m.gapEndId.Load() {
		// Gap has been filled. It is important to update gapEndId first
		// to make sure we don't create artifical Gap of [0..gapEnd] for a time.
		m.gapEndId.Store(0)
		m.gapStartId.Store(0)
		m.gapEndIdG.Set(0)
		m.gapStartIdG.Set(0)
		panic.OnErr(m.sess.TxBegin())
		panic.OnErr(m.metaC.Insert(m.mkGapStartId, make([]byte, 8)))
		panic.OnErr(m.metaC.Insert(m.mkGapEndId, make([]byte, 8)))
		panic.OnErr(m.sess.TxCommit())
	} else {
		m.gapStartId.Store(entryId)
		m.gapStartIdG.Set(float64(entryId))
		binary.BigEndian.PutUint64(m.buf8, uint64(entryId))
		panic.OnErr(m.metaC.Insert(m.mkGapStartId, m.buf8))
	}
}

func (m *streamStorage) CommitEntry(entryId int64, entryXX uint64) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}
	return m.commitEntry(entryId, entryXX, false)
}

// Updates committedEntry, assuming m.mx is acquired. Returns False, if entryId is too far in the future
// and local storage doesn't yet know about missing entries in between.
func (m *streamStorage) commitEntry(entryId int64, entryXX uint64, newGap bool) error {
	committed := m.committed.Load()
	if entryId <= committed {
		return nil
	}
	if entryId > m.tailEntry.EntryId {
		return status.Errorf(codes.OutOfRange, "commit entryId: %d > %d", entryId, m.tailEntry.EntryId)
	}
	existingEntryXX, ok := m.readEntryXX(entryId)
	if !ok {
		return status.Errorf(
			codes.DataLoss, "uncommitted entry %d: missing [%d..%d]", entryId, committed, m.tailEntry.EntryId)
	}
	if existingEntryXX != entryXX {
		return status.Errorf(
			codes.OutOfRange, "commit checksum mismatch for entry: %d, %d != %d, %v",
			entryId, entryXX, existingEntryXX, m.wInfo.Id)
	}
	if newGap {
		panic.OnNotOk(m.sess.InTx(), "new gap must happen inside a transaction")
		if m.gapStartId.Load() == 0 {
			// gapStartId must be updated before gapEndId.
			gapStartId := committed + 1
			m.gapStartId.Store(gapStartId)
			m.gapStartIdG.Set(float64(gapStartId))
			binary.BigEndian.PutUint64(m.buf8, uint64(gapStartId))
			panic.OnErr(m.metaC.Insert(m.mkGapStartId, m.buf8))
		}

		m.gapEndId.Store(entryId)
		m.gapEndIdG.Set(float64(entryId))
		binary.BigEndian.PutUint64(m.buf8, uint64(entryId))
		panic.OnErr(m.metaC.Insert(m.mkGapEndId, m.buf8))
	}

	m.committed.Store(entryId)
	m.committedIdG.Set(float64(entryId))
	if m.commitNotify != nil {
		// commitNotify must be updated after committedId.
		close(m.commitNotify)
		m.commitNotify = nil
	}
	binary.BigEndian.PutUint64(m.buf8, uint64(entryId))
	panic.OnErr(m.metaC.Insert(m.mkCommittedId, m.buf8))
	return nil
}

func (m *streamStorage) PutEntry(entry *walleapi.Entry, isCommitted bool) (bool, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return false, status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}

	// NOTE(zviad): if !isCommitted, writerId needs to be checked here again atomically, in the lock.
	if !isCommitted && CmpWriterIds(entry.WriterId, m.wInfo.Id) < 0 {
		return false, status.Errorf(codes.FailedPrecondition, "%v < %v", entry.WriterId, m.wInfo.Id)
	}
	if entry.EntryId > m.tailEntry.EntryId+1 {
		if !isCommitted {
			return false, status.Errorf(codes.OutOfRange, "put entryId: %d > %d + 1", entry.EntryId, m.tailEntry.EntryId)
		}
		m.makeGapCommit(entry)
		return true, nil
	}
	committed := m.committed.Load()
	if entry.EntryId < committed {
		return false, status.Errorf(codes.OutOfRange, "put entryId: %d < %d", entry.EntryId, committed)
	} else if entry.EntryId == committed {
		existing := m.readEntry(entry.EntryId)
		panic.OnNotOk(existing != nil, "committed entry missing: %d", entry.EntryId)
		if existing.ChecksumXX != entry.ChecksumXX {
			code := codes.OutOfRange
			if isCommitted {
				code = codes.DataLoss
			}
			return false, status.Errorf(
				code, "put checksum mismatch for committed entry: %d, %d != %d, %v",
				entry.EntryId, entry.ChecksumXX, existing.ChecksumXX, entry.WriterId)
		}
		if CmpWriterIds(existing.WriterId, entry.WriterId) >= 0 {
			return false, nil
		}
		m.insertEntry(entry)
		return false, nil
	}

	prevEntryXX, ok := m.readEntryXX(entry.EntryId - 1)
	if !ok {
		return false, status.Errorf(
			codes.DataLoss, "uncommitted entry %d: missing [%d..%d]", entry.EntryId-1, committed, m.tailEntry.EntryId)
	}
	expectedXX := wallelib.CalculateChecksumXX(prevEntryXX, entry.Data)
	if expectedXX != entry.ChecksumXX {
		if !isCommitted {
			return false, status.Errorf(
				codes.OutOfRange, "put checksum mismatch for new entry: %d, %d != %d, %v",
				entry.EntryId, entry.ChecksumXX, expectedXX, entry.WriterId)
		}
		if entry.EntryId-1 <= committed {
			// This mustn't happen. Otherwise probably a sign of data corruption or serious data
			// consistency bugs.
			return false, status.Errorf(
				codes.DataLoss, "put checksum mismatch for committed entry: %d, %d != %d, %v",
				entry.EntryId, entry.ChecksumXX, expectedXX, entry.WriterId)
		}
		m.makeGapCommit(entry)
		return true, nil
	}

	entryExists := false
	needsTrim := false
	if m.tailEntry.EntryId >= entry.EntryId {
		existingEntry := m.readEntry(entry.EntryId)
		cmpWriterId := CmpWriterIds(existingEntry.WriterId, entry.WriterId)
		if cmpWriterId > 0 {
			return false, status.Errorf(
				codes.OutOfRange, "put entry writer too old: %d, %v < %v",
				entry.EntryId, entry.WriterId, existingEntry.WriterId)
		}
		// Truncate entries, because rest of the uncommitted entries are no longer valid, since a new writer
		// is writing a previous entry.
		needsTrim = (cmpWriterId < 0)
		entryExists = (cmpWriterId == 0) && (existingEntry.ChecksumXX == entry.ChecksumXX)
	}

	if !isCommitted && m.tailEntry.EntryId-committed >= int64(len(m.tailEntryXX)) {
		// This should never happen unless client is really buggy. No client should allow
		// uncommitted entries to grow unbounded.
		return false, status.Errorf(
			codes.OutOfRange, "put entry can't succeed for: %d, too many uncommitted entries: %d .. %d",
			entry.EntryId, committed, m.tailEntry.EntryId)
	}

	if needsTrim {
		// If trimming is needed, it is important to run the whole operation in a transaction. Partial
		// commit of just trimming the entries without inserting the new entry can cause data corruption
		// otherwise.
		panic.OnErr(m.sess.TxBegin())
		m.removeAllEntriesFrom(entry.EntryId, entry)
	}
	if !entryExists {
		m.insertEntry(entry)
	}
	if isCommitted {
		err := m.commitEntry(entry.EntryId, entry.ChecksumXX, false)
		panic.OnErr(err) // This commit entry mustn't fail. Failure would mean data corrution.
	}
	if needsTrim {
		panic.OnErr(m.sess.TxCommit())
	}
	return false, nil
}

// Assumes m.mx is acquired.
func (m *streamStorage) makeGapCommit(entry *walleapi.Entry) {
	panic.OnErr(m.sess.TxBegin())
	committed := m.committed.Load()
	if m.tailEntry.EntryId > committed &&
		CmpWriterIds(m.tailEntry.WriterId, entry.WriterId) != 0 {
		// If writer has changed, need to clear out all uncommitted entries.
		// TODO(zviadm): Is there any danger to not clearing out entries when
		// writer is the same?
		m.removeAllEntriesFrom(committed+1, entry)
	}
	m.insertEntry(entry)
	err := m.commitEntry(entry.EntryId, entry.ChecksumXX, true)
	panic.OnErr(err) // This commit entry mustn't fail. Failure would mean data corrution.
	panic.OnErr(m.sess.TxCommit())
}

// readEntry reads entry from storage. Can return `nil` if entry is missing from local storage.
// Assumes m.mx is acquired.
func (m *streamStorage) readEntry(entryId int64) *walleapi.Entry {
	if entryId == m.tailEntry.EntryId {
		return m.tailEntry
	}
	binary.BigEndian.PutUint64(m.buf8, uint64(entryId))
	err := m.streamC.Search(m.buf8)
	if err != nil {
		panic.OnNotOk(wt.ErrCode(err) == wt.ErrNotFound, err.Error())
		return nil
	}
	entry := unmarshalValue(m.streamURI, m.committed.Load(), m.streamC)
	panic.OnErr(m.streamC.Reset())
	return entry
}
func (m *streamStorage) readEntryXX(entryId int64) (uint64, bool) {
	if entryId >= m.committed.Load() && entryId <= m.tailEntry.EntryId {
		return m.tailEntryXX[entryId%int64(len(m.tailEntryXX))], true
	}
	entry := m.readEntry(entryId)
	if entry == nil {
		return 0, false
	}
	return entry.ChecksumXX, true
}
func (m *streamStorage) insertEntry(entry *walleapi.Entry) {
	if entry.EntryId > m.tailEntry.EntryId {
		m.updateTailEntry(entry)
	}
	if entry.EntryId >= m.committed.Load() && entry.EntryId <= m.tailEntry.EntryId {
		m.tailEntryXX[entry.EntryId%int64(len(m.tailEntryXX))] = entry.ChecksumXX
	}

	binary.BigEndian.PutUint64(m.buf8, uint64(entry.EntryId))
	n, err := entry.MarshalTo(m.entryBuf)
	panic.OnErr(err)
	panic.OnErr(m.streamC.Insert(m.buf8, m.entryBuf[:n]))
}

// Deletes all entries [entryId...) from storage, and sets new tailEntry afterwards.
func (m *streamStorage) removeAllEntriesFrom(entryId int64, tailEntry *walleapi.Entry) {
	binary.BigEndian.PutUint64(m.buf8, uint64(entryId))
	panic.OnErr(m.streamC.Search(m.buf8))
	for {
		panic.OnErr(m.streamC.Remove())
		err := m.streamC.Next()
		if wt.ErrCode(err) == wt.ErrNotFound {
			break
		}
		panic.OnErr(err)
	}
	panic.OnErr(m.streamC.Reset())
	m.updateTailEntry(tailEntry)
}

func (m *streamStorage) updateTailEntry(e *walleapi.Entry) {
	m.tailEntry = e
	m.tailEntryId.Store(e.EntryId)
	m.tailIdG.Set(float64(m.tailEntry.EntryId))
}
