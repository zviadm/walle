package storage

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/wt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
