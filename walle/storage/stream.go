package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/zviadm/stats-go/metrics"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/wt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var flagExtraValidations = flag.Bool(
	"walle.storage.extra_validations", false,
	"If True, will perform extra validations and data sanity checks. This adversely affects performance, "+
		"only recommended for test and staging nodes, serving non production workloads.")

type streamStorage struct {
	serverId  string
	streamURI string
	sess      *wt.Session
	metaW     *wt.Mutator
	streamR   *wt.Scanner
	streamW   *wt.Mutator

	// separate read only session and lock for ReadFrom calls.
	roMX   sync.Mutex
	sessRO *wt.Session

	mx       sync.Mutex
	topology *walleapi.StreamTopology

	writerId        WriterId
	writerAddr      string
	writerLease     time.Duration
	renewedLease    time.Time
	committed       int64
	gapStartId      int64
	gapEndId        int64
	committedNotify chan struct{}

	entryBuf        *walleapi.Entry
	tailEntry       *walleapi.Entry
	tailEntryNotify chan struct{}
	buf8            []byte

	committedIdG metrics.Gauge
	gapStartIdG  metrics.Gauge
	gapEndIdG    metrics.Gauge
	tailIdG      metrics.Gauge
}

func createStreamStorage(
	serverId string,
	streamURI string,
	sess *wt.Session,
	sessRO *wt.Session) Stream {
	panic.OnErr(ValidateStreamURI(streamURI))
	panic.OnErr(sess.Create(streamDS(streamURI), wt.DataSourceCfg{BlockCompressor: "snappy"}))

	panic.OnErr(sess.TxBegin(wt.TxCfg{Sync: wt.True}))
	metaW, err := sess.Mutate(metadataDS)
	panic.OnErr(err)
	streamW, err := sess.Mutate(streamDS(streamURI))
	panic.OnErr(err)

	panic.OnErr(metaW.Insert([]byte(streamURI+sfxWriterId), make([]byte, writerIdLen)))
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxWriterAddr), []byte{}))
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxWriterLeaseNs), make([]byte, 8)))
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxCommittedId), make([]byte, 8)))
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxGapStartId), make([]byte, 8)))
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxGapEndId), make([]byte, 8)))
	panic.OnErr(streamW.Insert(make([]byte, 8), entry0B))
	panic.OnErr(sess.TxCommit())
	panic.OnErr(metaW.Close())
	panic.OnErr(streamW.Close())
	return openStreamStorage(serverId, streamURI, sess, sessRO)
}

func openStreamStorage(
	serverId string,
	streamURI string,
	sess *wt.Session,
	sessRO *wt.Session) Stream {
	metaR, err := sess.Scan(metadataDS)
	panic.OnErr(err)
	defer func() { panic.OnErr(metaR.Close()) }()
	metaW, err := sess.Mutate(metadataDS)
	panic.OnErr(err)
	r := &streamStorage{
		serverId:  serverId,
		streamURI: streamURI,

		sess:            sess,
		sessRO:          sessRO,
		metaW:           metaW,
		committedNotify: make(chan struct{}),

		buf8:            make([]byte, 8),
		tailEntry:       new(walleapi.Entry),
		tailEntryNotify: make(chan struct{}),

		committedIdG: committedIdGauge.V(metrics.KV{"stream_uri": streamURI}),
		gapStartIdG:  gapStartIdGauge.V(metrics.KV{"stream_uri": streamURI}),
		gapEndIdG:    gapEndIdGauge.V(metrics.KV{"stream_uri": streamURI}),
		tailIdG:      tailIdGauge.V(metrics.KV{"stream_uri": streamURI}),
	}
	v, err := metaR.ReadValue([]byte(streamURI + sfxWriterId))
	panic.OnErr(err)
	r.writerId = WriterId(v)
	v, err = metaR.ReadValue([]byte(streamURI + sfxWriterAddr))
	panic.OnErr(err)
	r.writerAddr = string(v)
	v, err = metaR.ReadValue([]byte(streamURI + sfxWriterLeaseNs))
	panic.OnErr(err)
	r.writerLease = time.Duration(binary.BigEndian.Uint64(v))

	v, err = metaR.ReadValue([]byte(streamURI + sfxCommittedId))
	panic.OnErr(err)
	r.committed = int64(binary.BigEndian.Uint64(v))
	r.committedIdG.Set(float64(r.committed))
	v, err = metaR.ReadValue([]byte(streamURI + sfxGapStartId))
	panic.OnErr(err)
	r.gapStartId = int64(binary.BigEndian.Uint64(v))
	r.gapStartIdG.Set(float64(r.gapStartId))
	v, err = metaR.ReadValue([]byte(streamURI + sfxGapEndId))
	panic.OnErr(err)
	r.gapEndId = int64(binary.BigEndian.Uint64(v))
	r.gapEndIdG.Set(float64(r.gapEndId))

	r.streamR, err = sess.Scan(streamDS(streamURI))
	panic.OnErr(err)
	defer func() { panic.OnErr(r.streamR.Reset()) }()
	r.streamW, err = sess.Mutate(streamDS(streamURI))
	panic.OnErr(err)
	nearType, err := r.streamR.SearchNear(maxEntryIdKey)
	panic.OnErr(err)
	panic.OnNotOk(nearType == wt.MatchedSmaller, "must return SmallerMatch when searching with maxEntryIdKey")
	r.tailEntry = unmarshalValue(r.streamURI, r.committed, r.streamR)
	r.tailIdG.Set(float64(r.tailEntry.EntryId))
	return r
}

func (m *streamStorage) StreamURI() string {
	return m.streamURI
}

func (m *streamStorage) close() {
	m.roMX.Lock()
	panic.OnErr(m.sessRO.Close())
	m.roMX.Unlock()

	m.mx.Lock()
	defer m.mx.Unlock()
	panic.OnErr(m.sess.Close())
	close(m.tailEntryNotify)
	close(m.committedNotify)
}
func (m *streamStorage) IsClosed() bool {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.sess.Closed()
}

func (m *streamStorage) Topology() *walleapi.StreamTopology {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.topology
}
func (m *streamStorage) setTopology(t *walleapi.StreamTopology) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.topology = t
}

func (m *streamStorage) WriterInfo() (WriterId, string, time.Duration, time.Duration) {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.writerId, m.writerAddr, m.writerLease, m.unsafeRemainingLease()
}
func (m *streamStorage) UpdateWriter(
	writerId WriterId, writerAddr string, lease time.Duration) (time.Duration, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return 0, status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}
	cmpWriterId := bytes.Compare(writerId, m.writerId)
	if cmpWriterId < 0 {
		return 0, status.Errorf(codes.FailedPrecondition, "%s < %s", writerId, m.writerId)
	}
	if cmpWriterId == 0 {
		return 0, nil
	}
	remainingLease := m.unsafeRemainingLease()
	m.writerId = writerId
	m.writerAddr = writerAddr
	m.writerLease = lease
	m.renewedLease = time.Now()

	panic.OnErr(m.sess.TxBegin(wt.TxCfg{Sync: wt.True}))
	panic.OnErr(m.metaW.Insert([]byte(m.streamURI+sfxWriterId), []byte(m.writerId)))
	panic.OnErr(m.metaW.Insert([]byte(m.streamURI+sfxWriterAddr), []byte(m.writerAddr)))
	binary.BigEndian.PutUint64(m.buf8, uint64(lease.Nanoseconds()))
	panic.OnErr(m.metaW.Insert([]byte(m.streamURI+sfxWriterLeaseNs), []byte(m.buf8)))
	panic.OnErr(m.sess.TxCommit())
	return remainingLease, nil
}
func (m *streamStorage) unsafeRemainingLease() time.Duration {
	return m.renewedLease.Add(m.writerLease).Sub(time.Now())
}
func (m *streamStorage) RenewLease(
	writerId WriterId, extraBuffer time.Duration) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	cmpWriterId := bytes.Compare(writerId, m.writerId)
	if cmpWriterId < 0 {
		return status.Errorf(codes.FailedPrecondition, "%s < %s", writerId, m.writerId)
	}
	if cmpWriterId > 0 {
		// RenewLease should never be called with newer writerId.
		return status.Errorf(codes.Internal, "renew lease: %s > %s", writerId, m.writerId)
	}
	panic.OnNotOk(extraBuffer >= 0, "extra buffer must be >=0: %s", extraBuffer)
	m.renewedLease = time.Now().Add(extraBuffer)
	return nil
}

func (m *streamStorage) TailEntries() ([]*walleapi.Entry, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return nil, status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}
	binary.BigEndian.PutUint64(m.buf8, uint64(m.committed))
	mType, err := m.streamR.SearchNear(m.buf8)
	panic.OnErr(err)
	panic.OnNotOk(mType == wt.MatchedExact, "committed entries mustn't have any gaps")
	r := make([]*walleapi.Entry, int(m.tailEntry.EntryId-m.committed+1))
	for idx := range r {
		r[idx] = unmarshalValue(m.streamURI, m.committed, m.streamR)
		panic.OnNotOk(
			r[idx].EntryId == m.committed+int64(idx),
			"tail entry missing: %s [%d..%d] %d",
			m.streamURI, m.committed, m.tailEntry.EntryId, idx)
		if idx != len(r)-1 {
			panic.OnErr(m.streamR.Next())
		} else {
			panic.OnErr(m.streamR.Reset())
		}
	}
	return r, nil
}

func (m *streamStorage) CommittedEntryId() (committedId int64, notify <-chan struct{}) {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.committed, m.committedNotify
}
func (m *streamStorage) TailEntryId() (int64, <-chan struct{}) {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.tailEntry.EntryId, m.tailEntryNotify
}
func (m *streamStorage) GapRange() (startId int64, endId int64) {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.gapStartId, m.gapEndId
}
func (m *streamStorage) UpdateGapStart(entryId int64) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() || entryId <= m.gapStartId {
		return
	}
	m.gapStartId = entryId
	if m.gapStartId >= m.gapEndId {
		m.gapStartId = 0 // caught up with committed.
		m.gapEndId = 0
		panic.OnErr(m.sess.TxBegin())
		panic.OnErr(m.metaW.Insert([]byte(m.streamURI+sfxGapStartId), make([]byte, 8)))
		panic.OnErr(m.metaW.Insert([]byte(m.streamURI+sfxGapEndId), make([]byte, 8)))
		panic.OnErr(m.sess.TxCommit())
	} else {
		binary.BigEndian.PutUint64(m.buf8, uint64(m.gapStartId))
		panic.OnErr(m.metaW.Insert([]byte(m.streamURI+sfxGapStartId), m.buf8))
	}
	m.gapStartIdG.Set(float64(m.gapStartId))
	m.gapEndIdG.Set(float64(m.gapEndId))
}

func (m *streamStorage) CommitEntry(entryId int64, entryMd5 []byte) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}
	return m.unsafeCommitEntry(entryId, entryMd5, false)
}

// Updates committedEntry, assuming m.mx is acquired. Returns False, if entryId is too far in the future
// and local storage doesn't yet know about missing entries in between.
func (m *streamStorage) unsafeCommitEntry(entryId int64, entryMd5 []byte, newGap bool) error {
	if entryId <= m.committed {
		return nil
	}
	if entryId > m.tailEntry.EntryId {
		return status.Errorf(codes.OutOfRange, "commit entryId: %d > %d", entryId, m.tailEntry.EntryId)
	}
	existingEntry := m.unsafeReadEntry(entryId)
	if existingEntry == nil {
		return status.Errorf(
			codes.Internal, "uncommitted entry %d: missing [%d..%d]", entryId, m.committed, m.tailEntry.EntryId)
	}
	if bytes.Compare(existingEntry.ChecksumMd5, entryMd5) != 0 {
		return status.Errorf(
			codes.OutOfRange, "commit checksum mismatch for entry: %d, %s != %s, %s vs %s",
			entryId, hex.EncodeToString(entryMd5), hex.EncodeToString(existingEntry.ChecksumMd5),
			m.writerId, WriterId(existingEntry.WriterId))
	}
	if newGap {
		panic.OnNotOk(m.sess.InTx(), "new gap must happen inside a transaction")
		if m.gapStartId == 0 {
			m.gapStartId = m.committed + 1
			m.gapStartIdG.Set(float64(m.gapStartId))
		}
		binary.BigEndian.PutUint64(m.buf8, uint64(m.gapStartId))
		panic.OnErr(m.metaW.Insert([]byte(m.streamURI+sfxGapStartId), m.buf8))

		m.gapEndId = entryId
		m.gapEndIdG.Set(float64(m.gapEndId))
		binary.BigEndian.PutUint64(m.buf8, uint64(m.gapEndId))
		panic.OnErr(m.metaW.Insert([]byte(m.streamURI+sfxGapEndId), m.buf8))
	}

	m.committed = entryId
	m.committedIdG.Set(float64(m.committed))
	close(m.committedNotify)
	m.committedNotify = make(chan struct{})
	binary.BigEndian.PutUint64(m.buf8, uint64(m.committed))
	panic.OnErr(m.metaW.Insert([]byte(m.streamURI+sfxCommittedId), m.buf8))
	return nil
}

func (m *streamStorage) PutEntry(entry *walleapi.Entry, isCommitted bool) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}

	entryWriterId := WriterId(entry.WriterId)
	panic.OnNotOk(len(entryWriterId) > 0, "writerId must always be set")
	// NOTE(zviad): if !isCommitted, writerId needs to be checked here again atomically, in the lock.
	if !isCommitted && bytes.Compare(entryWriterId, m.writerId) < 0 {
		return status.Errorf(codes.FailedPrecondition, "%s < %s", entryWriterId, m.writerId)
	}
	if entry.EntryId > m.tailEntry.EntryId+1 {
		if !isCommitted {
			return status.Errorf(codes.OutOfRange, "put entryId: %d > %d + 1", entry.EntryId, m.tailEntry.EntryId)
		}
		m.unsafeMakeGapCommit(entry)
		return nil
	}
	if entry.EntryId <= m.committed {
		if !isCommitted {
			return status.Errorf(codes.OutOfRange, "put entryId: %d <= %d", entry.EntryId, m.committed)
		}
		if entry.EntryId < m.gapStartId {
			return nil // not missing, and not last committed entry.
		}
		if entry.EntryId == m.committed {
			e := m.unsafeReadEntry(entry.EntryId)
			if e != nil && bytes.Compare(e.WriterId, entry.WriterId) >= 0 {
				return nil
			}
		}
		m.unsafeInsertEntry(entry)
		return nil
	}

	prevEntry := m.unsafeReadEntry(entry.EntryId - 1)
	panic.OnNotOk(prevEntry != nil, "gap in uncommitted entries!")
	expectedMd5 := wallelib.CalculateChecksumMd5(prevEntry.ChecksumMd5, entry.Data)
	if bytes.Compare(expectedMd5, entry.ChecksumMd5) != 0 {
		if !isCommitted {
			return status.Errorf(
				codes.OutOfRange, "put checksum mismatch for entry: %d, %s != %s, %s vs %s",
				entry.EntryId, hex.EncodeToString(entry.ChecksumMd5), hex.EncodeToString(expectedMd5),
				entryWriterId, WriterId(prevEntry.WriterId))
		}
		if prevEntry.EntryId <= m.committed {
			// This mustn't happen. Otherwise probably a sign of data corruption or serious data
			// consistency bugs.
			return status.Errorf(
				codes.Internal, "put checksum mismatch for committed entry: %d, %s != %s, %s vs %s",
				entry.EntryId, hex.EncodeToString(entry.ChecksumMd5), hex.EncodeToString(expectedMd5),
				entryWriterId, WriterId(prevEntry.WriterId))
		}
		m.unsafeMakeGapCommit(entry)
		return nil
	}

	entryExists := false
	needsTrim := false
	if m.tailEntry.EntryId >= entry.EntryId {
		existingEntry := m.unsafeReadEntry(entry.EntryId)
		cmpWriterId := bytes.Compare(existingEntry.WriterId, entryWriterId)
		if cmpWriterId > 0 {
			return status.Errorf(
				codes.OutOfRange, "put entry writer too old: %d, %s < %s",
				entry.EntryId, entryWriterId, WriterId(existingEntry.WriterId))
		}
		// Truncate entries, because rest of the uncommitted entries are no longer valid, since a new writer
		// is writing a previous entry.
		needsTrim = (cmpWriterId < 0)
		entryExists = (cmpWriterId == 0) && (bytes.Compare(existingEntry.ChecksumMd5, entry.ChecksumMd5) == 0)
	}
	if needsTrim {
		// If trimming is needed, it is important to run the whole operation in a transaction. Partial
		// commit of just trimming the entries without inserting the new entry can cause data corruption
		// otherwise.
		panic.OnErr(m.sess.TxBegin())
		m.unsafeRemoveAllEntriesFrom(entry.EntryId, entry)
	}
	if !entryExists {
		m.unsafeInsertEntry(entry)
	}
	if isCommitted {
		err := m.unsafeCommitEntry(entry.EntryId, entry.ChecksumMd5, false)
		panic.OnErr(err) // This commit entry mustn't fail. Failure would mean data corrution.
	}
	if needsTrim {
		panic.OnErr(m.sess.TxCommit())
	}
	return nil
}

func (m *streamStorage) unsafeMakeGapCommit(entry *walleapi.Entry) {
	// Clear out all uncommitted entries, and create a GAP.
	panic.OnErr(m.sess.TxBegin())
	m.unsafeRemoveAllEntriesFrom(m.committed+1, entry)
	m.unsafeInsertEntry(entry)
	err := m.unsafeCommitEntry(entry.EntryId, entry.ChecksumMd5, true)
	panic.OnErr(err) // This commit entry mustn't fail. Failure would mean data corrution.
	panic.OnErr(m.sess.TxCommit())
}

// Reads entry from storage. Can return `nil` if entry is missing from local storage.
// Assumes m.mx is acquired.
func (m *streamStorage) unsafeReadEntry(entryId int64) *walleapi.Entry {
	if entryId == m.tailEntry.EntryId {
		return m.tailEntry
	}
	binary.BigEndian.PutUint64(m.buf8, uint64(entryId))
	err := m.streamR.Search(m.buf8)
	if err != nil {
		panic.OnNotOk(wt.ErrCode(err) == wt.ErrNotFound, err.Error())
		return nil
	}
	entry := unmarshalValue(m.streamURI, m.committed, m.streamR)
	panic.OnErr(m.streamR.Reset())
	return entry
}
func (m *streamStorage) unsafeInsertEntry(entry *walleapi.Entry) {
	if entry.EntryId > m.tailEntry.EntryId {
		m.unsafeUpdateTailEntry(entry)
	}
	binary.BigEndian.PutUint64(m.buf8, uint64(entry.EntryId))
	entryB, err := entry.Marshal()
	panic.OnErr(err)
	if *flagExtraValidations {
		validateEntry := new(walleapi.Entry)
		panic.OnErr(validateEntry.Unmarshal(entryB))
	}
	panic.OnErr(m.streamW.Insert(m.buf8, entryB))
}

// Deletes all entries [entryId...) from storage, and sets new tailEntry afterwards.
func (m *streamStorage) unsafeRemoveAllEntriesFrom(entryId int64, tailEntry *walleapi.Entry) {
	for eId := entryId; eId <= m.tailEntry.EntryId; eId++ {
		binary.BigEndian.PutUint64(m.buf8, uint64(eId))
		panic.OnErr(m.streamW.Remove(m.buf8))
	}
	m.unsafeUpdateTailEntry(tailEntry)
}

func (m *streamStorage) unsafeUpdateTailEntry(e *walleapi.Entry) {
	if e.EntryId != m.tailEntry.EntryId {
		close(m.tailEntryNotify)
		m.tailEntryNotify = make(chan struct{})
	}
	m.tailEntry = e
	m.tailIdG.Set(float64(m.tailEntry.EntryId))
}

func (m *streamStorage) ReadFrom(entryId int64) (Cursor, error) {
	committedId, _ := m.CommittedEntryId()
	m.roMX.Lock()
	defer m.roMX.Unlock()
	if m.sessRO.Closed() {
		return nil, status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}
	cursor, err := m.sessRO.Scan(streamDS(m.streamURI))
	panic.OnErr(err)
	r := &streamCursor{
		roMX:        &m.roMX,
		sessRO:      m.sessRO,
		streamURI:   m.streamURI,
		cursor:      cursor,
		committedId: committedId,
		needsNext:   false,
	}
	var buf8 [8]byte
	binary.BigEndian.PutUint64(buf8[:], uint64(entryId))
	mType, err := cursor.SearchNear(buf8[:])
	panic.OnErr(err)
	if mType == wt.MatchedSmaller {
		r.needsNext = true
	}
	return r, nil
}

type streamCursor struct {
	roMX        *sync.Mutex
	sessRO      *wt.Session
	streamURI   string
	cursor      *wt.Scanner
	needsNext   bool
	finished    bool
	committedId int64
}

func (m *streamCursor) Close() {
	m.roMX.Lock()
	defer m.roMX.Unlock()
	m.close()
}
func (m *streamCursor) close() {
	if !m.sessRO.Closed() && !m.finished {
		panic.OnErr(m.cursor.Close())
	}
	m.finished = true
}
func (m *streamCursor) Next() (int64, bool) {
	m.roMX.Lock()
	defer m.roMX.Unlock()
	if m.sessRO.Closed() || m.finished {
		return 0, false
	}
	if m.needsNext {
		if err := m.cursor.Next(); err != nil {
			panic.OnNotOk(wt.ErrCode(err) == wt.ErrNotFound, err.Error())
			m.close()
			return 0, false
		}
	}
	m.needsNext = true
	unsafeKey, err := m.cursor.UnsafeKey() // This is safe because it is copied to entryId.
	panic.OnErr(err)
	entryId := int64(binary.BigEndian.Uint64(unsafeKey))
	if entryId > m.committedId {
		m.close()
		return 0, false
	}
	return entryId, true
}
func (m *streamCursor) Entry() *walleapi.Entry {
	m.roMX.Lock()
	defer m.roMX.Unlock()
	entry := unmarshalValue(m.streamURI, m.committedId, m.cursor)
	return entry
}

// Helper function to unmarshal value that scanner is currently pointing at. Scanner
// must be pointing at a valid record.
// streamURI & committedId are only needed to produce more detailed `panic` message if
// bug or data corruption has occured and entry can't be unmarshalled.
func unmarshalValue(streamURI string, committedId int64, c *wt.Scanner) *walleapi.Entry {
	unsafeV, err := c.UnsafeValue() // This is safe because Unmarshal call makes a copy.
	panic.OnErr(err)
	entry := new(walleapi.Entry)
	if err := entry.Unmarshal(unsafeV); err != nil {
		unsafeK, errK := c.UnsafeKey()
		panic.OnErr(errK)
		entryId := int64(binary.BigEndian.Uint64(unsafeK))
		panic.OnNotOk(false, fmt.Sprintf(
			"unmarshall %s: c%d, err: %s\nk: %v (%d)\nv (%d): %v",
			streamURI, committedId, err, unsafeK, entryId, len(unsafeV), unsafeV))
	}
	return entry
}
