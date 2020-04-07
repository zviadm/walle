package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"sync"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/wt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
		tailEntry:       &walleapi.Entry{},
		tailEntryNotify: make(chan struct{}),
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
	v, err = metaR.ReadValue([]byte(streamURI + sfxGapStartId))
	panic.OnErr(err)
	r.gapStartId = int64(binary.BigEndian.Uint64(v))
	v, err = metaR.ReadValue([]byte(streamURI + sfxGapEndId))
	panic.OnErr(err)
	r.gapEndId = int64(binary.BigEndian.Uint64(v))

	r.streamR, err = sess.Scan(streamDS(streamURI))
	panic.OnErr(err)
	defer func() { panic.OnErr(r.streamR.Reset()) }()
	r.streamW, err = sess.Mutate(streamDS(streamURI))
	panic.OnErr(err)
	nearType, err := r.streamR.SearchNear(maxEntryIdKey)
	panic.OnErr(err)
	panic.OnNotOk(nearType == wt.MatchedSmaller, "must return SmallerMatch when searching with maxEntryIdKey")
	v, err = r.streamR.Value()
	panic.OnErr(err)
	panic.OnErr(r.tailEntry.Unmarshal(v))
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
	writerId WriterId, writerAddr string, lease time.Duration) (bool, time.Duration) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return false, 0
	}
	cmpWriterId := bytes.Compare(writerId, m.writerId)
	if cmpWriterId <= 0 {
		return cmpWriterId == 0, 0
	}
	remainingLease := m.unsafeRemainingLease()
	m.writerId = writerId
	m.writerAddr = writerAddr
	m.writerLease = lease
	m.renewedLease = time.Now()

	panic.OnErr(m.sess.TxBegin(wt.TxCfg{Sync: wt.True}))
	panic.OnErr(m.metaW.Update([]byte(m.streamURI+sfxWriterId), []byte(m.writerId)))
	panic.OnErr(m.metaW.Update([]byte(m.streamURI+sfxWriterAddr), []byte(m.writerAddr)))
	binary.BigEndian.PutUint64(m.buf8, uint64(lease.Nanoseconds()))
	panic.OnErr(m.metaW.Update([]byte(m.streamURI+sfxWriterLeaseNs), []byte(m.buf8)))
	panic.OnErr(m.sess.TxCommit())
	return true, remainingLease
}
func (m *streamStorage) unsafeRemainingLease() time.Duration {
	return m.renewedLease.Add(m.writerLease).Sub(time.Now())
}
func (m *streamStorage) RenewLease(
	writerId WriterId, extraBuffer time.Duration) bool {
	m.mx.Lock()
	defer m.mx.Unlock()
	if bytes.Compare(writerId, m.writerId) != 0 {
		return false
	}
	panic.OnNotOk(extraBuffer >= 0, "extra buffer must be >=0: %s", extraBuffer)
	m.renewedLease = time.Now().Add(extraBuffer)
	return true
}

func (m *streamStorage) TailEntries() ([]*walleapi.Entry, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return nil, status.Errorf(codes.NotFound, "stream: %s not found locally", m.streamURI)
	}
	binary.BigEndian.PutUint64(m.buf8, uint64(m.committed))
	mType, err := m.streamR.SearchNear(m.buf8)
	panic.OnErr(err)
	panic.OnNotOk(mType == wt.MatchedExact, "committed entries mustn't have any gaps")
	r := make([]*walleapi.Entry, int(m.tailEntry.EntryId-m.committed+1))
	for idx := range r {
		unsafeV, err := m.streamR.UnsafeValue()
		panic.OnErr(err)
		entry := &walleapi.Entry{}
		panic.OnErr(entry.Unmarshal(unsafeV))
		r[idx] = entry
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
		m.gapStartId = m.committed // caught up with committed.
	}
	binary.BigEndian.PutUint64(m.buf8, uint64(m.gapStartId))
	panic.OnErr(m.metaW.Update([]byte(m.streamURI+sfxGapStartId), m.buf8))
}

func (m *streamStorage) CommitEntry(entryId int64, entryMd5 []byte) bool {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return false
	}
	return m.unsafeCommitEntry(entryId, entryMd5, false, true)
}

// Updates committedEntry, assuming m.mx is acquired. Returns False, if entryId is too far in the future
// and local storage doesn't yet know about missing entries in between.
func (m *streamStorage) unsafeCommitEntry(entryId int64, entryMd5 []byte, newGap bool, useTx bool) bool {
	if entryId <= m.committed {
		return true
	}
	if entryId > m.tailEntry.EntryId {
		return false
	}
	existingEntry := m.unsafeReadEntry(entryId)
	if existingEntry == nil {
		return false
	}
	panic.OnNotOk(
		bytes.Compare(existingEntry.ChecksumMd5, entryMd5) == 0,
		"committed entry md5 mimstach at: %d, %s vs %s",
		entryId, hex.EncodeToString(entryMd5), hex.EncodeToString(existingEntry.ChecksumMd5))
	panic.OnNotOk(useTx || m.sess.InTx(), "commit must happen inside a transaction")
	if useTx {
		panic.OnErr(m.sess.TxBegin())
	}
	if newGap {
		m.gapEndId = entryId
	} else if m.gapStartId >= m.committed {
		m.gapStartId = entryId
		binary.BigEndian.PutUint64(m.buf8, uint64(m.gapStartId))
		panic.OnErr(m.metaW.Update([]byte(m.streamURI+sfxGapStartId), m.buf8))
	}
	m.committed = entryId
	close(m.committedNotify)
	m.committedNotify = make(chan struct{})
	binary.BigEndian.PutUint64(m.buf8, uint64(m.committed))
	panic.OnErr(m.metaW.Update([]byte(m.streamURI+sfxCommittedId), m.buf8))
	if useTx {
		panic.OnErr(m.sess.TxCommit())
	}
	return true
}

func (m *streamStorage) PutEntry(entry *walleapi.Entry, isCommitted bool) bool {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() {
		return false
	}

	entryWriterId := WriterId(entry.WriterId)
	panic.OnNotOk(len(entryWriterId) > 0, "writerId must always be set")
	// NOTE(zviad): if !isCommitted, writerId needs to be checked here again atomically, in the lock.
	if !isCommitted && bytes.Compare(entryWriterId, m.writerId) != 0 {
		return false
	}
	if entry.EntryId > m.tailEntry.EntryId+1 {
		if !isCommitted {
			return false
		}
		m.unsafeMakeGapCommit(entry)
		return true
	}
	if entry.EntryId <= m.committed {
		if !isCommitted {
			return false
		}
		if entry.EntryId < m.gapStartId {
			return true // not missing, and not last committed entry.
		}
		if entry.EntryId == m.committed {
			e := m.unsafeReadEntry(entry.EntryId)
			if e != nil && bytes.Compare(e.WriterId, entry.WriterId) >= 0 {
				return true // entry exists and is written by same or newer writerId.
			}
		}
		m.unsafeInsertEntry(entry)
		return true
	}

	prevEntry := m.unsafeReadEntry(entry.EntryId - 1)
	panic.OnNotOk(prevEntry != nil, "gap in uncommitted entries!")
	expectedMd5 := wallelib.CalculateChecksumMd5(prevEntry.ChecksumMd5, entry.Data)
	if bytes.Compare(expectedMd5, entry.ChecksumMd5) != 0 {
		if !isCommitted {
			return false
		}
		panic.OnNotOk(
			prevEntry.EntryId > m.committed, "committed entry checksum mismatch!")
		m.unsafeMakeGapCommit(entry)
		return true
	}

	entryExists := false
	needsTrim := false
	if m.tailEntry.EntryId >= entry.EntryId {
		existingEntry := m.unsafeReadEntry(entry.EntryId)
		cmpWriterId := bytes.Compare(existingEntry.WriterId, entryWriterId)
		if cmpWriterId > 0 {
			return false
		}
		// Truncate entries, because rest of the uncommitted entries are no longer valid, since a new writer
		// is writing a previous entry.
		needsTrim = (cmpWriterId < 0)
		entryExists = (cmpWriterId == 0) && (bytes.Compare(existingEntry.ChecksumMd5, entry.ChecksumMd5) == 0)
	}
	panic.OnErr(m.sess.TxBegin())
	if needsTrim {
		m.unsafeRemoveAllEntriesFrom(entry.EntryId, entry)
	}
	if !entryExists {
		m.unsafeInsertEntry(entry)
	}
	if isCommitted {
		ok := m.unsafeCommitEntry(entry.EntryId, entry.ChecksumMd5, false, false)
		panic.OnNotOk(ok, "committing must always succeed in this code path")
	}
	panic.OnErr(m.sess.TxCommit())
	return true
}

func (m *streamStorage) unsafeMakeGapCommit(entry *walleapi.Entry) {
	// Clear out all uncommitted entries, and create a GAP.
	panic.OnErr(m.sess.TxBegin())
	m.unsafeRemoveAllEntriesFrom(m.committed+1, entry)
	m.unsafeInsertEntry(entry)
	ok := m.unsafeCommitEntry(entry.EntryId, entry.ChecksumMd5, true, false)
	panic.OnNotOk(ok, "committing must always succeed in this code path")
	panic.OnErr(m.sess.TxCommit())
}

// Reads entry from storage. Can return `nil` if entry is missing from local storage.
// Assumes m.mx is acquired.
func (m *streamStorage) unsafeReadEntry(entryId int64) *walleapi.Entry {
	if entryId == m.tailEntry.EntryId {
		return m.tailEntry
	}
	binary.BigEndian.PutUint64(m.buf8, uint64(entryId))
	unsafeV, err := m.streamR.ReadUnsafeValue(m.buf8)
	if err != nil && wt.ErrCode(err) == wt.ErrNotFound {
		return nil
	}
	panic.OnErr(err)
	entry := &walleapi.Entry{}
	panic.OnErr(entry.Unmarshal(unsafeV))
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
}

func (m *streamStorage) ReadFrom(entryId int64) (Cursor, error) {
	committedId, _ := m.CommittedEntryId()
	m.roMX.Lock()
	defer m.roMX.Unlock()
	if m.sess.Closed() {
		return nil, status.Errorf(codes.NotFound, "stream: %s not found locally", m.streamURI)
	}
	cursor, err := m.sessRO.Scan(streamDS(m.streamURI))
	panic.OnErr(err)
	r := &streamCursor{
		mx:          &m.roMX,
		sess:        m.sessRO,
		cursor:      cursor,
		committedId: committedId,
	}
	var buf8 [8]byte
	binary.BigEndian.PutUint64(buf8[:], uint64(entryId))
	mType, err := cursor.SearchNear(buf8[:])
	panic.OnErr(err)
	if mType == wt.MatchedSmaller {
		_, _ = r.skip()
	}
	return r, nil
}

type streamCursor struct {
	mx          *sync.Mutex
	sess        *wt.Session
	cursor      *wt.Scanner
	finished    bool
	committedId int64
}

func (m *streamCursor) Close() {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.close()
}
func (m *streamCursor) close() {
	if !m.sess.Closed() && !m.finished {
		panic.OnErr(m.cursor.Close())
	}
	m.finished = true
}
func (m *streamCursor) Skip() (int64, bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.skip()
}
func (m *streamCursor) skip() (int64, bool) {
	if m.sess.Closed() || m.finished {
		return 0, false
	}
	unsafeKey, err := m.cursor.UnsafeKey()
	panic.OnErr(err)
	entryId := int64(binary.BigEndian.Uint64(unsafeKey))
	if entryId > m.committedId {
		m.close()
		return 0, false
	}
	if err := m.cursor.Next(); err != nil {
		if wt.ErrCode(err) != wt.ErrNotFound {
			panic.OnErr(err)
		}
		m.close()
	}
	return entryId, true
}
func (m *streamCursor) Next() (*walleapi.Entry, bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.sess.Closed() || m.finished {
		return nil, false
	}
	unsafeV, err := m.cursor.UnsafeValue()
	panic.OnErr(err)
	entry := &walleapi.Entry{}
	panic.OnErr(entry.Unmarshal(unsafeV))
	if entry.EntryId > m.committedId {
		m.close()
		return nil, false
	}
	if err := m.cursor.Next(); err != nil {
		if wt.ErrCode(err) != wt.ErrNotFound {
			panic.OnErr(err)
		}
		m.close()
	}
	return entry, true
}
