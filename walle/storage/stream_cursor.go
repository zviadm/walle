package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/zviadm/stats-go/metrics"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/wt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *streamStorage) ReadFrom(entryId int64) (Cursor, error) {
	committedId := m.CommittedId()
	m.roMX.Lock()
	defer m.roMX.Unlock()
	if m.sessRO.Closed() {
		return nil, status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}
	m.cursorsG.Add(1)
	r := &streamCursor{
		roMX:         &m.roMX,
		sessRO:       m.sessRO,
		streamURI:    m.streamURI,
		cursorsG:     m.cursorsG,
		cursorNextsC: m.cursorNextsC,
		committedId:  committedId,
	}
	cfg := wt.CursorCfg{ReadOnce: wt.True, Readonly: wt.True}
	cursor0, err := m.sessRO.OpenCursor(streamDS(m.streamURI), cfg)
	panic.OnErr(err)
	cursor1, err := m.sessRO.OpenCursor(streamBackfillDS(m.streamURI), cfg)
	panic.OnErr(err)
	cursors := []*wtCursor{{c: cursor0}, {c: cursor1}}
	for _, c := range cursors {
		binary.BigEndian.PutUint64(m.roBuf8, uint64(entryId))
		mType, err := c.c.SearchNear(m.roBuf8)
		if err != nil {
			panic.OnNotOk(wt.ErrCode(err) == wt.ErrNotFound, err.Error())
			c.close()
		} else if mType == wt.MatchedSmaller {
			c.needsNext = true
		}
	}
	r.cursors = cursors
	panic.OnNotOk(len(cursors) <= 2, "current code only works for combining at most 2 cursors!")
	return r, nil
}

// streamCursor reads from both `stream` and `stream_backfill` tables and merges
// results to provide single stream of sequential entries. Never returns same entry
// more than once, even if there is overlap between `stream` and `stream_backfill` tables.
type streamCursor struct {
	roMX         *sync.Mutex
	sessRO       *wt.Session
	streamURI    string
	cursorsG     metrics.Gauge
	cursorNextsC metrics.Counter

	committedId int64
	finished    bool
	current     *wtCursor
	cursors     []*wtCursor
}

type wtCursor struct {
	c         *wt.Cursor
	needsNext bool
	finished  bool
}

func (c *wtCursor) close() {
	if !c.finished {
		panic.OnErr(c.c.Close())
	}
	c.finished = true
}

func (m *streamCursor) Close() {
	m.roMX.Lock()
	defer m.roMX.Unlock()
	m.close()
}
func (m *streamCursor) close() {
	if !m.sessRO.Closed() && !m.finished {
		m.cursorsG.Add(-1)
		for _, c := range m.cursors {
			c.close()
		}
	}
	m.finished = true
}
func (m *streamCursor) Next() (int64, bool) {
	m.roMX.Lock()
	defer m.roMX.Unlock()
	if m.sessRO.Closed() || m.finished {
		return 0, false
	}
	m.cursorNextsC.Count(1)
	m.current = nil
	var nextEntryId int64 = math.MaxInt64
	for _, c := range m.cursors {
		if c.finished {
			continue
		}
		if c.needsNext {
			if err := c.c.Next(); err != nil {
				panic.OnNotOk(wt.ErrCode(err) == wt.ErrNotFound, err.Error())
				c.close()
				continue
			}
			c.needsNext = false
		}
		unsafeKey, err := c.c.UnsafeKey() // This is safe because it is copied to entryId.
		panic.OnErr(err)
		entryId := int64(binary.BigEndian.Uint64(unsafeKey))
		if entryId < nextEntryId {
			nextEntryId = entryId
			m.current = c
		} else if entryId == nextEntryId {
			c.needsNext = true
		}
	}
	if m.current == nil {
		m.close()
		return 0, false
	}
	m.current.needsNext = true
	if nextEntryId > m.committedId {
		m.close()
		return 0, false
	}
	return nextEntryId, true
}
func (m *streamCursor) Entry() *walleapi.Entry {
	m.roMX.Lock()
	defer m.roMX.Unlock()
	entry := unmarshalValue(m.streamURI, m.committedId, m.current.c)
	return entry
}

// Helper function to unmarshal value that cursor is currently pointing at.
// streamURI & committedId are only needed to produce more detailed `panic` message if
// bug or data corruption has occured and entry can't be unmarshalled.
func unmarshalValue(streamURI string, committedId int64, c *wt.Cursor) *walleapi.Entry {
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
