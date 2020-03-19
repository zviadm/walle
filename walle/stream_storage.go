package walle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
	"github.com/zviadm/wt"
)

type streamStorage struct {
	serverId  string
	streamURI string
	sess      *wt.Session // can be nil for testing.
	metaW     *wt.Mutator
	streamR   *wt.Scanner
	streamW   *wt.Mutator

	mx       sync.Mutex
	topology *walleapi.StreamTopology

	writerId        string
	committed       int64
	noGapCommitted  int64
	committedNotify chan struct{}

	entryKeyBuf []byte
	entryBuf    *walleapi.Entry
	tailEntry   *walleapi.Entry
}

func createStreamStorage(
	serverId string,
	streamURI string,
	topology *walleapi.StreamTopology,
	sess *wt.Session) StreamStorage {
	panicOnErr(
		isValidStreamURI(streamURI))
	panicOnErr(
		sess.Create(streamDS(streamURI), &wt.DataSourceConfig{BlockCompressor: "snappy"}))

	panicOnErr(sess.TxBegin())
	metaW, err := sess.Mutate(metadataDS, nil)
	panicOnErr(err)
	streamW, err := sess.Mutate(streamDS(streamURI), nil)
	panicOnErr(err)

	v, err := topology.Marshal()
	panicOnErr(err)
	panicOnErr(
		metaW.Insert([]byte(streamURI+sfxTopology), v))
	panicOnErr(
		metaW.Insert([]byte(streamURI+sfxWriterId), make([]byte, writerIdLen)))
	panicOnErr(
		metaW.Insert([]byte(streamURI+sfxCommittedId), make([]byte, 8)))
	panicOnErr(
		metaW.Insert([]byte(streamURI+sfxNoGapCommittedId), make([]byte, 8)))
	panicOnErr(
		streamW.Insert(make([]byte, 8), entry0B))
	panicOnErr(sess.TxCommit())
	panicOnErr(metaW.Close())
	panicOnErr(streamW.Close())
	return openStreamStorage(serverId, streamURI, sess)
}

func openStreamStorage(serverId string, streamURI string, sess *wt.Session) StreamStorage {
	metaR, err := sess.Scan(metadataDS)
	panicOnErr(err)
	defer func() { panicOnErr(metaR.Close()) }()
	streamR, err := sess.Scan(streamDS(streamURI))
	panicOnErr(err)
	defer func() { panicOnErr(streamR.Reset()) }()

	metaW, err := sess.Mutate(metadataDS, nil)
	panicOnErr(err)
	streamW, err := sess.Mutate(streamDS(streamURI), nil)
	panicOnErr(err)
	r := &streamStorage{
		serverId:  serverId,
		streamURI: streamURI,

		sess:            sess,
		metaW:           metaW,
		streamR:         streamR,
		streamW:         streamW,
		topology:        &walleapi.StreamTopology{},
		committedNotify: make(chan struct{}),

		entryKeyBuf: make([]byte, 8),
		tailEntry:   &walleapi.Entry{},
	}
	v, err := metaR.ReadUnsafeValue([]byte(streamURI + sfxTopology))
	panicOnErr(err)
	panicOnErr(
		r.topology.Unmarshal(v))
	v, err = metaR.ReadUnsafeValue([]byte(streamURI + sfxWriterId))
	panicOnErr(err)
	r.writerId = string(v)
	v, err = metaR.ReadUnsafeValue([]byte(streamURI + sfxCommittedId))
	panicOnErr(err)
	r.committed = int64(binary.BigEndian.Uint64(v))
	v, err = metaR.ReadUnsafeValue([]byte(streamURI + sfxNoGapCommittedId))
	panicOnErr(err)
	r.noGapCommitted = int64(binary.BigEndian.Uint64(v))
	nearType, err := streamR.SearchNear(maxEntryIdKey)
	panicOnErr(err)
	panicOnNotOk(nearType == wt.SmallerMatch, "must return SmallerMatch when searching with maxEntryIdKey")
	v, err = streamR.UnsafeValue()
	panicOnErr(err)
	panicOnErr(
		r.tailEntry.Unmarshal(v))
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
	panicOnErr(err)
	panicOnErr(
		m.metaW.Update([]byte(m.streamURI+sfxTopology), v))
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
	return false
}

func (m *streamStorage) StreamURI() string {
	return m.streamURI
}

func (m *streamStorage) WriterId() string {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.writerId
}

func (m *streamStorage) UpdateWriterId(writerId string) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if writerId <= m.writerId {
		return
	}
	m.writerId = writerId
	panicOnErr(
		m.metaW.Update([]byte(m.streamURI+sfxWriterId), []byte(m.writerId)))
}

func (m *streamStorage) LastEntries() []*walleapi.Entry {
	m.mx.Lock()
	defer m.mx.Unlock()
	binary.BigEndian.PutUint64(m.entryKeyBuf, uint64(m.committed))
	mType, err := m.streamR.SearchNear(m.entryKeyBuf)
	panicOnErr(err)
	panicOnNotOk(mType == wt.ExactMatch, "committed entries mustn't have any gaps")
	r := make([]*walleapi.Entry, int(m.tailEntry.EntryId-m.committed+1))
	for idx := range r {
		v, err := m.streamR.UnsafeValue()
		panicOnErr(err)
		entry := &walleapi.Entry{}
		panicOnErr(
			entry.Unmarshal(v))
		r[idx] = entry
		if idx != len(r)-1 {
			panicOnErr(m.streamR.Next())
		} else {
			panicOnErr(m.streamR.Reset())
		}
	}
	return r
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
	binary.BigEndian.PutUint64(m.entryKeyBuf, uint64(m.noGapCommitted))
	panicOnErr(
		m.metaW.Update([]byte(m.streamURI+sfxNoGapCommittedId), m.entryKeyBuf))
}

func (m *streamStorage) CommitEntry(entryId int64, entryMd5 []byte) bool {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.unsafeCommitEntry(entryId, entryMd5, false)
}

// Updates committedEntry, assuming m.mx is acquired. Returns False, if entryId is too far in the future
// and local storage doesn't yet know about missing entries in between.
func (m *streamStorage) unsafeCommitEntry(entryId int64, entryMd5 []byte, newGap bool) bool {
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
	panicOnNotOk(
		bytes.Compare(existingEntry.ChecksumMd5, entryMd5) == 0,
		fmt.Sprintf("committed entry md5 mimstach at: %d, %v vs %v", entryId, entryMd5, existingEntry.ChecksumMd5))
	if !newGap && m.noGapCommitted == m.committed {
		m.noGapCommitted = entryId
	}
	m.committed = entryId
	close(m.committedNotify)
	m.committedNotify = make(chan struct{})
	binary.BigEndian.PutUint64(m.entryKeyBuf, uint64(m.committed))
	panicOnErr(
		m.metaW.Update([]byte(m.streamURI+sfxCommittedId), m.entryKeyBuf))
	return true
}

func (m *streamStorage) PutEntry(entry *walleapi.Entry, isCommitted bool) bool {
	m.mx.Lock()
	defer m.mx.Unlock()

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
		e := m.unsafeReadEntry(entry.EntryId)
		if e == nil {
			m.unsafeInsertEntry(entry)
			return true
		}
		panicOnNotOk(
			bytes.Compare(e.ChecksumMd5, entry.ChecksumMd5) == 0,
			"committed entry checksum mismatch!")
		return true
	}

	prevEntry := m.unsafeReadEntry(entry.EntryId - 1)
	panicOnNotOk(prevEntry != nil, "gap in uncommitted entries!")
	expectedMd5 := wallelib.CalculateChecksumMd5(prevEntry.ChecksumMd5, entry.Data)
	if bytes.Compare(expectedMd5, entry.ChecksumMd5) != 0 {
		if !isCommitted {
			return false
		}
		m.unsafeMakeGapCommit(entry)
		return true
	}

	// NOTE(zviad): if !isCommitted, writerId needs to be checked here again atomically, in the lock.
	if !isCommitted && entry.WriterId != m.writerId {
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
		panicOnErr(m.sess.TxBegin())
		defer func() { panicOnErr(m.sess.TxCommit()) }()
		m.unsafeRemoveAllEntriesFrom(entry.EntryId, entry)
	}
	m.unsafeInsertEntry(entry)
	if isCommitted {
		ok := m.unsafeCommitEntry(entry.EntryId, entry.ChecksumMd5, false)
		panicOnNotOk(ok, "committing must always succeed in this code path")
	}
	return true
}

func (m *streamStorage) unsafeMakeGapCommit(entry *walleapi.Entry) {
	// Clear out all uncommitted entries, and create a GAP.
	panicOnErr(m.sess.TxBegin())
	defer func() { panicOnErr(m.sess.TxCommit()) }()
	m.unsafeRemoveAllEntriesFrom(m.committed+1, entry)
	m.unsafeInsertEntry(entry)
	ok := m.unsafeCommitEntry(entry.EntryId, entry.ChecksumMd5, true)
	panicOnNotOk(ok, "committing must always succeed in this code path")
}

// Reads entry from storage. Can return `nil` if entry is missing from local storage.
// Assumes m.mx is acquired.
func (m *streamStorage) unsafeReadEntry(entryId int64) *walleapi.Entry {
	if entryId == m.tailEntry.EntryId {
		return m.tailEntry
	}
	binary.BigEndian.PutUint64(m.entryKeyBuf, uint64(entryId))
	v, err := m.streamR.ReadUnsafeValue(m.entryKeyBuf)
	if err != nil && wt.ErrCode(err) == wt.ErrNotFound {
		return nil
	}
	panicOnErr(err)
	entry := &walleapi.Entry{}
	panicOnErr(entry.Unmarshal(v))
	panicOnErr(m.streamR.Reset())
	return entry
}
func (m *streamStorage) unsafeInsertEntry(entry *walleapi.Entry) {
	if entry.EntryId > m.tailEntry.EntryId {
		m.tailEntry = entry
	}
	binary.BigEndian.PutUint64(m.entryKeyBuf, uint64(entry.EntryId))
	entryB, err := entry.Marshal()
	panicOnErr(err)
	panicOnErr(
		m.streamW.Insert(m.entryKeyBuf, entryB))
}

// Deletes all entries [entryId...) from storage, and sets new tailEntry afterwards.
func (m *streamStorage) unsafeRemoveAllEntriesFrom(entryId int64, tailEntry *walleapi.Entry) {
	for eId := entryId; eId <= m.tailEntry.EntryId; eId++ {
		binary.BigEndian.PutUint64(m.entryKeyBuf, uint64(eId))
		panicOnErr(m.streamW.Remove(m.entryKeyBuf))
	}
	m.tailEntry = tailEntry
}

func (m *streamStorage) ReadFrom(entryId int64) StreamCursor {
	m.mx.Lock()
	defer m.mx.Unlock()
	cursor, err := m.sess.Scan(streamDS(m.streamURI))
	panicOnErr(err)
	binary.BigEndian.PutUint64(m.entryKeyBuf, uint64(entryId))
	mType, err := cursor.SearchNear(m.entryKeyBuf)
	panicOnErr(err)
	if mType == wt.SmallerMatch {
		cursor.Next() // TODO(zviad): needs a check?
	}
	return &streamCursor{
		mx:      &m.mx,
		cursor:  cursor,
		entryId: entryId,
	}
}

type streamCursor struct {
	mx      *sync.Mutex
	cursor  *wt.Scanner
	entryId int64
}

func (m *streamCursor) Close() {
	_ = m.cursor.Close()
}
func (m *streamCursor) Next() (*walleapi.Entry, bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	v, err := m.cursor.UnsafeValue()
	if err != nil && wt.ErrCode(err) == wt.ErrNotFound {
		return nil, false
	}
	entry := &walleapi.Entry{}
	panicOnErr(entry.Unmarshal(v))
	m.cursor.Next()
	return entry, true
}
