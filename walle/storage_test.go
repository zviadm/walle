package walle

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
)

func TestStorageOpen(t *testing.T) {
	dbPath := TestTmpDir()
	s, err := StorageInit(dbPath, true)
	require.NoError(t, err)
	s.NewStream("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	s.Close()

	s, err = StorageInit(dbPath, false)
	require.NoError(t, err)
	defer s.Close()
	require.EqualValues(t, s.Streams(false), []string{"/s/1"})
}

func TestStreamStorage(t *testing.T) {
	s, err := StorageInit(TestTmpDir(), true)
	require.NoError(t, err)
	defer s.Close()
	ss := s.NewStream("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})

	var entries []*walleapi.Entry
	entries = append(entries, entry0)
	for idx := 1; idx <= 5; idx++ {
		entry := &walleapi.Entry{
			EntryId:  int64(idx),
			WriterId: entry0.WriterId,
			Data:     []byte("entry " + strconv.Itoa(idx)),
		}
		entry.ChecksumMd5 = wallelib.CalculateChecksumMd5(entries[idx-1].ChecksumMd5, entry.Data)
		entries = append(entries, entry)
	}

	ok := ss.PutEntry(entries[1], false)
	require.True(t, ok)
	noGap, committed, _ := ss.CommittedEntryIds()
	require.EqualValues(t, 0, noGap)
	require.EqualValues(t, 0, committed)

	entriesR := streamReadAll(t, ss, 0)
	require.EqualValues(t, 1, len(entriesR)) // entry1 shouldn't be visible yet.
	require.EqualValues(t, 0, entriesR[0].EntryId)

	ok = ss.PutEntry(entries[3], false)
	require.False(t, ok)

	ok = ss.PutEntry(entries[3], true)
	require.True(t, ok)
	noGap, committed, _ = ss.CommittedEntryIds()
	require.EqualValues(t, 0, noGap)
	require.EqualValues(t, 3, committed)

	entriesR = streamReadAll(t, ss, 0)
	require.EqualValues(t, 2, len(entriesR)) // entry1 should have been removed because of Gap commit.
	require.EqualValues(t, 0, entriesR[0].EntryId)
	require.EqualValues(t, 3, entriesR[1].EntryId)

	entriesR = streamReadAll(t, ss, 1)
	require.EqualValues(t, 1, len(entriesR))
	require.EqualValues(t, 3, entriesR[0].EntryId)

	c0 := ss.ReadFrom(1)
	ok = ss.PutEntry(entries[5], true)
	require.True(t, ok)
	entry, ok := c0.Next()
	require.True(t, ok)
	require.EqualValues(t, 3, entry.EntryId)
	c0.Close()

	c0 = ss.ReadFrom(1)
	c1 := ss.ReadFrom(5)
	entry, ok = c1.Next()
	require.True(t, ok)
	require.EqualValues(t, 5, entry.EntryId)
	entry, ok = c0.Next()
	require.True(t, ok)
	require.EqualValues(t, 3, entry.EntryId)
	c0.Close()
	c0.Close() // check to make sure it is safe to close closed cursor
	c1.Close()
}

func streamReadAll(t *testing.T, ss StreamStorage, entryId int64) []*walleapi.Entry {
	cursor := ss.ReadFrom(entryId)
	defer cursor.Close()
	var r []*walleapi.Entry
	for {
		v, ok := cursor.Next()
		if !ok {
			_, ok = cursor.Next()
			require.False(t, ok)
			break
		}
		r = append(r, v)
	}
	return r
}
