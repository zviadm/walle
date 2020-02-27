package walle

import (
	"bytes"
	"crypto/md5"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
)

type mockStorage struct {
	serverId string
	mx       sync.Mutex
	streams  map[string]*mockStream
}

type mockStream struct {
	serverId  string
	streamURI string

	mx       sync.Mutex
	topology *walleapi.StreamTopology
	isLocal  bool

	writerId        string
	entries         []*walleapi.Entry
	committed       int64
	noGapCommitted  int64
	committedNotify chan struct{}
}

var _ Storage = &mockStorage{}

func newMockStorage(serverId string) *mockStorage {
	return &mockStorage{
		serverId: serverId,
		streams:  make(map[string]*mockStream),
	}
}

func (m *mockStorage) Streams(localOnly bool) []string {
	m.mx.Lock()
	defer m.mx.Unlock()
	r := make([]string, 0, len(m.streams))
	for streamURI, s := range m.streams {
		if localOnly && !s.IsLocal() {
			continue
		}
		r = append(r, streamURI)
	}
	return r
}

func (m *mockStorage) Stream(streamURI string, localOnly bool) (StreamStorage, bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	r, ok := m.streams[streamURI]
	if ok && localOnly && !r.IsLocal() {
		return nil, false
	}
	return r, ok
}

func (m *mockStorage) NewStream(streamURI string, t *walleapi.StreamTopology) {
	s := &mockStream{
		serverId:        m.serverId,
		streamURI:       streamURI,
		entries:         []*walleapi.Entry{&walleapi.Entry{ChecksumMd5: make([]byte, md5.Size)}},
		committedNotify: make(chan struct{}),
	}
	s.UpdateTopology(t)

	m.mx.Lock()
	defer m.mx.Unlock()
	m.streams[streamURI] = s
}

func (m *mockStream) Topology() *walleapi.StreamTopology {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.topology
}

func (m *mockStream) UpdateTopology(topology *walleapi.StreamTopology) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.topology = topology
	m.isLocal = false
	for _, serverId := range m.topology.ServerIds {
		if serverId == m.serverId {
			m.isLocal = true
			break
		}
	}
}

func (m *mockStream) IsLocal() bool {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.isLocal
}

func (m *mockStream) StreamURI() string {
	return m.streamURI
}

func (m *mockStream) WriterId() string {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.writerId
}

func (m *mockStream) UpdateWriterId(writerId string) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if writerId <= m.writerId {
		return
	}
	m.writerId = writerId
}

func (m *mockStream) LastEntries() []*walleapi.Entry {
	m.mx.Lock()
	defer m.mx.Unlock()
	r := m.entries[int(m.committed):len(m.entries)]
	rCopy := make([]*walleapi.Entry, len(r))
	for idx, entry := range r {
		rCopy[idx] = proto.Clone(entry).(*walleapi.Entry)
	}
	return rCopy
}

func (m *mockStream) CommittedEntryIds() (noGapCommittedIt int64, committedId int64, notify <-chan struct{}) {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.noGapCommitted, m.committed, m.committedNotify
}
func (m *mockStream) UpdateNoGapCommittedId(entryId int64) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if entryId > m.noGapCommitted {
		m.noGapCommitted = entryId
		close(m.committedNotify)
		m.committedNotify = make(chan struct{})
	}
}

func (m *mockStream) CommitEntry(entryId int64, entryMd5 []byte) bool {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.unsafeCommitEntry(entryId, entryMd5, false)
}

func (m *mockStream) unsafeCommitEntry(entryId int64, entryMd5 []byte, newGap bool) bool {
	if entryId <= m.committed {
		return true
	}
	if entryId >= int64(len(m.entries)) {
		return false
	}
	if bytes.Compare(m.entries[entryId].ChecksumMd5, entryMd5) != 0 {
		return false
	}
	if !newGap && m.noGapCommitted == m.committed {
		m.noGapCommitted = entryId
	}
	m.committed = entryId
	close(m.committedNotify)
	m.committedNotify = make(chan struct{})
	return true
}

func (m *mockStream) PutEntry(entry *walleapi.Entry, isCommitted bool) bool {
	m.mx.Lock()
	defer m.mx.Unlock()

	if entry.EntryId > int64(len(m.entries)) {
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
		e := m.entries[int(entry.EntryId)]
		if e == nil {
			m.entries[int(entry.EntryId)] = entry
			return true
		}
		if bytes.Compare(e.ChecksumMd5, entry.ChecksumMd5) != 0 {
			panic("DeveloperError; committed entry checksum mismatch!")
		}
		return true
	}

	prevEntry := m.entries[int(entry.EntryId)-1]
	if prevEntry == nil {
		panic("DeveloperError; GAP in uncommitted entries!")
	}
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
	if int64(len(m.entries)) > entry.EntryId {
		existingEntry := m.entries[int(entry.EntryId)]
		if existingEntry.WriterId > entry.WriterId {
			return false
		}
		if existingEntry.WriterId == entry.WriterId {
			return true
		}
		// Truncate entries, because rest of the uncommitted entries are no longer valid, since a new writer
		// is writing a new entry.
		m.entries = m.entries[:int(entry.EntryId)]
	}
	m.entries = append(m.entries, entry)
	if isCommitted {
		m.unsafeCommitEntry(entry.EntryId, entry.ChecksumMd5, false)
	}
	return true
}

func (m *mockStream) unsafeMakeGapCommit(entry *walleapi.Entry) {
	// Clear out all uncommitted entries, and create a GAP.
	if int64(len(m.entries)) > entry.EntryId {
		m.entries = m.entries[:int(entry.EntryId)]
	}
	for idx := int(m.committed) + 1; idx < len(m.entries); idx++ {
		m.entries[idx] = nil
	}
	for int64(len(m.entries)) < entry.EntryId {
		m.entries = append(m.entries, nil)
	}
	m.entries = append(m.entries, entry)
	ok := m.unsafeCommitEntry(entry.EntryId, entry.ChecksumMd5, true)
	if !ok {
		panic("DeveloperError; unreachable code reached!")
	}
}

func (m *mockStream) ReadFrom(entryId int64) StreamCursor {
	return &mockCursor{m: m, entryId: entryId}
}

type mockCursor struct {
	m       *mockStream
	entryId int64
}

func (m *mockCursor) Close() {}
func (m *mockCursor) Next() (*walleapi.Entry, bool) {
	m.m.mx.Lock()
	defer m.m.mx.Unlock()
	for m.entryId < int64(len(m.m.entries)) {
		e := m.m.entries[m.entryId]
		m.entryId += 1
		if e != nil {
			return e, true
		}
	}
	return nil, false
}
