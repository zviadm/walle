package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
)

func TestStorageOpen(t *testing.T) {
	dbPath := TestTmpDir()
	s, err := Init(dbPath, true)
	require.NoError(t, err)
	s.NewStream("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	s.Close()

	s, err = Init(dbPath, false)
	require.NoError(t, err)
	defer s.Close()
	require.EqualValues(t, s.Streams(false), []string{"/s/1"})
}

func TestStreamStorage(t *testing.T) {
	s, err := Init(TestTmpDir(), true)
	require.NoError(t, err)
	defer s.Close()
	ss := s.NewStream("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})

	var entries []*walleapi.Entry
	entries = append(entries, Entry0)
	for idx := 1; idx <= 5; idx++ {
		entry := &walleapi.Entry{
			EntryId:  int64(idx),
			WriterId: Entry0.WriterId,
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

	entriesR = streamReadAll(t, ss, 10)
	require.EqualValues(t, 0, len(entriesR))

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

func TestStreamStorageRaces(t *testing.T) {
	s, err := Init(TestTmpDir(), true)
	require.NoError(t, err)
	defer s.Close()
	ss := s.NewStream("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})

	ctx, cancel := context.WithCancel(context.Background())
	errC := make(chan error, 1)
	nReaders := 3
	for i := 0; i < nReaders; i++ {
		go func() (err error) {
			defer func() { errC <- err }()
			for ctx.Err() == nil {
				_ = streamReadAll(t, ss, 0)
			}
			return nil
		}()
	}
	entry := Entry0
	for idx := 1; idx <= 50; idx++ {
		data := []byte("entry " + strconv.Itoa(idx))
		checksum := wallelib.CalculateChecksumMd5(entry.ChecksumMd5, data)
		entry = &walleapi.Entry{
			EntryId:     int64(idx),
			WriterId:    entry.WriterId,
			Data:        data,
			ChecksumMd5: checksum,
		}
		ok := ss.PutEntry(entry, true)
		require.True(t, ok)
	}
	cancel()
	for i := 0; i < nReaders; i++ {
		err = <-errC
	}
	require.NoError(t, err)
}

func streamReadAll(t *testing.T, ss Stream, entryId int64) []*walleapi.Entry {
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
