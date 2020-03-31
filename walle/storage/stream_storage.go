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
	noGapCommitted  int64
	committedNotify chan struct{}

	entryBuf        *walleapi.Entry
	tailEntry       *walleapi.Entry
	tailEntryNotify chan struct{}
	buf8            []byte
}

func createStreamStorage(
	serverId string,
	streamURI string,
	topology *walleapi.StreamTopology,
	sess *wt.Session,
	sessRO *wt.Session) Stream {
	panic.OnErr(isValidStreamURI(streamURI))
	panic.OnErr(sess.Create(streamDS(streamURI), &wt.DataSourceConfig{BlockCompressor: "snappy"}))

	panic.OnErr(sess.TxBegin(&wt.TxConfig{Sync: wt.True}))
	metaW, err := sess.Mutate(metadataDS, nil)
	panic.OnErr(err)
	streamW, err := sess.Mutate(streamDS(streamURI), nil)
	panic.OnErr(err)

	v, err := topology.Marshal()
	panic.OnErr(err)
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxTopology), v))
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxWriterId), make([]byte, writerIdLen)))
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxWriterAddr), []byte{}))
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxWriterLeaseNs), make([]byte, 8)))
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxCommittedId), make([]byte, 8)))
	panic.OnErr(metaW.Insert([]byte(streamURI+sfxNoGapCommittedId), make([]byte, 8)))
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
	streamR, err := sess.Scan(streamDS(streamURI))
	panic.OnErr(err)
	defer func() { panic.OnErr(streamR.Reset()) }()

	metaW, err := sess.Mutate(metadataDS, nil)
	panic.OnErr(err)
	streamW, err := sess.Mutate(streamDS(streamURI), nil)
	panic.OnErr(err)
	r := &streamStorage{
		serverId:  serverId,
		streamURI: streamURI,

		sess:            sess,
		sessRO:          sessRO,
		metaW:           metaW,
		streamR:         streamR,
		streamW:         streamW,
		topology:        &walleapi.StreamTopology{},
		committedNotify: make(chan struct{}),

		buf8:            make([]byte, 8),
		tailEntry:       &walleapi.Entry{},
		tailEntryNotify: make(chan struct{}),
	}
	v, err := metaR.ReadUnsafeValue([]byte(streamURI + sfxTopology))
	panic.OnErr(err)
	panic.OnErr(r.topology.Unmarshal(v))
	v, err = metaR.ReadUnsafeValue([]byte(streamURI + sfxWriterId))
	panic.OnErr(err)
	r.writerId = WriterId(v)
	v, err = metaR.ReadUnsafeValue([]byte(streamURI + sfxWriterAddr))
	panic.OnErr(err)
	r.writerAddr = string(v)
	v, err = metaR.ReadUnsafeValue([]byte(streamURI + sfxWriterLeaseNs))
	panic.OnErr(err)
	r.writerLease = time.Duration(binary.BigEndian.Uint64(v))
	v, err = metaR.ReadUnsafeValue([]byte(streamURI + sfxCommittedId))
	panic.OnErr(err)
	r.committed = int64(binary.BigEndian.Uint64(v))
	v, err = metaR.ReadUnsafeValue([]byte(streamURI + sfxNoGapCommittedId))
	panic.OnErr(err)
	r.noGapCommitted = int64(binary.BigEndian.Uint64(v))
	nearType, err := streamR.SearchNear(maxEntryIdKey)
	panic.OnErr(err)
	panic.OnNotOk(nearType == wt.SmallerMatch, "must return SmallerMatch when searching with maxEntryIdKey")
	v, err = streamR.UnsafeValue()
	panic.OnErr(err)
	panic.OnErr(r.tailEntry.Unmarshal(v))
	return r
}

func (m *streamStorage) Topology() *walleapi.StreamTopology {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.topology
}

func (m *streamStorage) UpdateTopology(topology *walleapi.StreamTopology) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if topology.Version < m.topology.GetVersion() {
		return
	}
	m.topology = topology
	v, err := m.topology.Marshal()
	panic.OnErr(err)

	panic.OnErr(m.sess.TxBegin(&wt.TxConfig{Sync: wt.True}))
	panic.OnErr(
		m.metaW.Update([]byte(m.streamURI+sfxTopology), v))
	panic.OnErr(m.sess.TxCommit())
}

func (m *streamStorage) IsLocal() bool {
	m.mx.Lock()
	defer m.mx.Unlock()
	// TODO(zviad): should this be cached for speed up?
	for _, serverId := range m.topology.ServerIds {
		if serverId == m.serverId {
			return true
		}
	}
	for _, serverId := range m.topology.PrevServerIds {
		if serverId == m.serverId {
			return true
		}
	}
	return false
}

func (m *streamStorage) StreamURI() string {
	return m.streamURI
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
	if writerId <= m.writerId {
		return writerId == m.writerId, 0
	}
	remainingLease := m.unsafeRemainingLease()
	m.writerId = writerId
	m.writerAddr = writerAddr
	m.writerLease = lease
	m.renewedLease = time.Now()

	panic.OnErr(m.sess.TxBegin(&wt.TxConfig{Sync: wt.True}))
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
func (m *streamStorage) RenewLease(writerId WriterId) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if writerId == m.writerId {
		m.renewedLease = time.Now()
	}
}

func (m *streamStorage) LastEntries() []*walleapi.Entry {
	m.mx.Lock()
	defer m.mx.Unlock()
	binary.BigEndian.PutUint64(m.buf8, uint64(m.committed))
	mType, err := m.streamR.SearchNear(m.buf8)
	panic.OnErr(err)
	panic.OnNotOk(mType == wt.ExactMatch, "committed entries mustn't have any gaps")
	r := make([]*walleapi.Entry, int(m.tailEntry.EntryId-m.committed+1))
	for idx := range r {
		v, err := m.streamR.UnsafeValue()
		panic.OnErr(err)
		entry := &walleapi.Entry{}
		panic.OnErr(
			entry.Unmarshal(v))
		r[idx] = entry
		if idx != len(r)-1 {
			panic.OnErr(m.streamR.Next())
		} else {
			panic.OnErr(m.streamR.Reset())
		}
	}
	return r
}

func (m *streamStorage) TailEntryId() (int64, <-chan struct{}) {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.tailEntry.EntryId, m.tailEntryNotify
}

func (m *streamStorage) CommittedEntryIds() (noGapCommittedIt int64, committedId int64, notify <-chan struct{}) {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.noGapCommitted, m.committed, m.committedNotify
}
func (m *streamStorage) UpdateNoGapCommittedId(entryId int64) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if entryId <= m.noGapCommitted {
		return
	}
	m.noGapCommitted = entryId
	close(m.committedNotify)
	m.committedNotify = make(chan struct{})
	binary.BigEndian.PutUint64(m.buf8, uint64(m.noGapCommitted))
	panic.OnErr(
		m.metaW.Update([]byte(m.streamURI+sfxNoGapCommittedId), m.buf8))
}

func (m *streamStorage) CommitEntry(entryId int64, entryMd5 []byte) bool {
	m.mx.Lock()
	defer m.mx.Unlock()
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
		panic.OnErr(m.sess.TxBegin(nil))
	}
	if !newGap && m.noGapCommitted == m.committed {
		m.noGapCommitted = entryId
		binary.BigEndian.PutUint64(m.buf8, uint64(m.noGapCommitted))
		panic.OnErr(m.metaW.Update([]byte(m.streamURI+sfxNoGapCommittedId), m.buf8))
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

	entryWriterId := WriterId(entry.WriterId)
	panic.OnNotOk(entryWriterId != "", "writerId must always be set")
	if !isCommitted && entryWriterId < m.writerId {
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
		if entry.EntryId <= m.noGapCommitted {
			return true
		}
		e := m.unsafeReadEntry(entry.EntryId)
		if e == nil {
			m.unsafeInsertEntry(entry)
			return true
		}
		panic.OnNotOk(
			bytes.Compare(e.ChecksumMd5, entry.ChecksumMd5) == 0,
			"committed entry md5 mimstach at: %d, %s vs %s",
			entry.EntryId, hex.EncodeToString(entry.ChecksumMd5), hex.EncodeToString(e.ChecksumMd5))
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

	// NOTE(zviad): if !isCommitted, writerId needs to be checked here again atomically, in the lock.
	if !isCommitted && entryWriterId != m.writerId {
		return false
	}
	if m.tailEntry.EntryId >= entry.EntryId {
		existingEntry := m.unsafeReadEntry(entry.EntryId)
		if existingEntry.WriterId > entry.WriterId {
			return false
		}
		if existingEntry.WriterId == entry.WriterId {
			return true
		}
		// Truncate entries, because rest of the uncommitted entries are no longer valid, since a new writer
		// is writing a new entry.
		panic.OnErr(m.sess.TxBegin(nil))
		m.unsafeRemoveAllEntriesFrom(entry.EntryId, entry)
	} else {
		panic.OnErr(m.sess.TxBegin(nil))
	}
	// Run this code path in a transaction to reduce overall number of `fsync`-s needed in the
	// most common/expected case.
	m.unsafeInsertEntry(entry)
	if isCommitted {
		ok := m.unsafeCommitEntry(entry.EntryId, entry.ChecksumMd5, false, false)
		panic.OnNotOk(ok, "committing must always succeed in this code path")
	}
	panic.OnErr(m.sess.TxCommit())
	return true
}

func (m *streamStorage) unsafeMakeGapCommit(entry *walleapi.Entry) {
	// Clear out all uncommitted entries, and create a GAP.
	panic.OnErr(m.sess.TxBegin(nil))
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
	v, err := m.streamR.ReadUnsafeValue(m.buf8)
	if err != nil && wt.ErrCode(err) == wt.ErrNotFound {
		return nil
	}
	panic.OnErr(err)
	entry := &walleapi.Entry{}
	panic.OnErr(entry.Unmarshal(v))
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

func (m *streamStorage) ReadFrom(entryId int64) Cursor {
	_, committedId, _ := m.CommittedEntryIds()
	m.roMX.Lock()
	defer m.roMX.Unlock()
	cursor, err := m.sessRO.Scan(streamDS(m.streamURI))
	panic.OnErr(err)
	r := &streamCursor{
		mx:          &m.roMX,
		cursor:      cursor,
		committedId: committedId,
	}
	var buf8 [8]byte
	binary.BigEndian.PutUint64(buf8[:], uint64(entryId))
	mType, err := cursor.SearchNear(buf8[:])
	panic.OnErr(err)
	if mType == wt.SmallerMatch {
		_, _ = r.skip()
	}
	return r
}

type streamCursor struct {
	mx          *sync.Mutex
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
	if !m.finished {
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
	if m.finished {
		return 0, false
	}
	v, err := m.cursor.UnsafeKey()
	panic.OnErr(err)
	entryId := int64(binary.BigEndian.Uint64(v))
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
	if m.finished {
		return nil, false
	}
	v, err := m.cursor.UnsafeValue()
	panic.OnErr(err)
	entry := &walleapi.Entry{}
	panic.OnErr(entry.Unmarshal(v))
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
