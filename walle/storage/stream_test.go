package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
)

func TestStreamStorage(t *testing.T) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true})
	require.NoError(t, err)
	defer s.Close()
	err = s.UpsertStream("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(t, err)
	ss, ok := s.Stream("/s/1")
	require.True(t, ok)

	var entries []*walleapi.Entry
	entries = append(entries, Entry0)
	writerId := MakeWriterId()
	_, err = ss.UpdateWriter(writerId, "", 0)
	require.NoError(t, err)
	for idx := 1; idx <= 5; idx++ {
		if idx == 3 {
			// Entries starting at idx:3 have new writerId
			writerId = MakeWriterId()
		}
		entry := &walleapi.Entry{
			EntryId:  int64(idx),
			WriterId: writerId,
			Data:     []byte("entry " + strconv.Itoa(idx)),
		}
		entry.ChecksumXX = wallelib.CalculateChecksumXX(entries[idx-1].ChecksumXX, entry.Data)
		entries = append(entries, entry)
	}

	_, err = ss.PutEntry(entries[1], false)
	require.NoError(t, err)
	committed := ss.CommittedId()
	require.EqualValues(t, 0, committed)
	gapStart, gapEnd := ss.GapRange()
	require.EqualValues(t, 0, gapStart)
	require.EqualValues(t, 0, gapEnd)

	entriesR := streamReadAll(t, ss, 0)
	require.EqualValues(t, 1, len(entriesR)) // entry1 shouldn't be visible yet.
	require.EqualValues(t, 0, entriesR[0].EntryId)

	_, err = ss.PutEntry(entries[3], false)
	require.EqualValues(t, codes.OutOfRange, status.Convert(err).Code())

	_, err = ss.PutEntry(entries[3], true)
	require.NoError(t, err)
	committed = ss.CommittedId()
	require.EqualValues(t, 3, committed)
	gapStart, gapEnd = ss.GapRange()
	require.EqualValues(t, 1, gapStart)
	require.EqualValues(t, 3, gapEnd)

	entriesR = streamReadAll(t, ss, 0)
	require.EqualValues(t, 2, len(entriesR)) // entry1 should have been removed because of Gap commit.
	require.EqualValues(t, 0, entriesR[0].EntryId)
	require.EqualValues(t, 3, entriesR[1].EntryId)

	entriesR = streamReadAll(t, ss, 1)
	require.EqualValues(t, 1, len(entriesR))
	require.EqualValues(t, 3, entriesR[0].EntryId)

	entriesR = streamReadAll(t, ss, 10)
	require.EqualValues(t, 0, len(entriesR))

	c0, err := ss.ReadFrom(1)
	require.NoError(t, err)
	_, err = ss.PutEntry(entries[5], true)
	require.NoError(t, err)
	entryId, ok := c0.Next()
	require.True(t, ok)
	require.EqualValues(t, 3, entryId)
	c0.Close()

	c0, err = ss.ReadFrom(1)
	require.NoError(t, err)
	c1, err := ss.ReadFrom(5)
	require.NoError(t, err)
	entryId, ok = c1.Next()
	require.True(t, ok)
	require.EqualValues(t, 5, entryId)
	entryId, ok = c0.Next()
	require.True(t, ok)
	require.EqualValues(t, 3, entryId)
	c0.Close()
	c0.Close() // check to make sure it is safe to close closed cursor
	c1.Close()

	// Make sure putting last committed entry again with new WriterId,
	// causes WriterId to update.
	entry5new := proto.Clone(entries[5]).(*walleapi.Entry)
	entry5new.WriterId = MakeWriterId()
	_, err = ss.UpdateWriter(entry5new.WriterId, "", 0)
	require.NoError(t, err)
	_, err = ss.PutEntry(entry5new, true)
	require.NoError(t, err)
	entriesR = streamReadAll(t, ss, 5)
	require.Len(t, entriesR, 1)
	require.EqualValues(t, entry5new.WriterId, entriesR[0].WriterId)
}

func TestStreamStorageRaces(t *testing.T) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true})
	require.NoError(t, err)
	defer s.Close()
	err = s.UpsertStream("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(t, err)
	ss, ok := s.Stream("/s/1")
	require.True(t, ok)

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
		checksum := wallelib.CalculateChecksumXX(entry.ChecksumXX, data)
		entry = &walleapi.Entry{
			EntryId:    int64(idx),
			WriterId:   entry.WriterId,
			Data:       data,
			ChecksumXX: checksum,
		}
		_, err := ss.PutEntry(entry, true)
		require.NoError(t, err)
	}
	cancel()
	for i := 0; i < nReaders; i++ {
		err = <-errC
	}
	require.NoError(t, err)
}

func TestStreamCursorWithGap(t *testing.T) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true})
	require.NoError(t, err)
	defer s.Close()
	err = s.UpsertStream("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(t, err)
	ss, ok := s.Stream("/s/1")
	require.True(t, ok)

	var entries []*walleapi.Entry
	entries = append(entries, Entry0)
	writerId := MakeWriterId()
	_, err = ss.UpdateWriter(writerId, "", 0)
	require.NoError(t, err)
	for idx := 1; idx <= 10; idx++ {
		entry := &walleapi.Entry{
			EntryId:  int64(idx),
			WriterId: writerId,
			Data:     []byte("entry " + strconv.Itoa(idx)),
		}
		entry.ChecksumXX = wallelib.CalculateChecksumXX(entries[idx-1].ChecksumXX, entry.Data)
		entries = append(entries, entry)
	}

	_, err = ss.PutEntry(entries[1], true)
	require.NoError(t, err)
	_, err = ss.PutEntry(entries[2], true)
	require.NoError(t, err)
	_, err = ss.PutEntry(entries[5], true)
	require.NoError(t, err)
	committed := ss.CommittedId()
	require.EqualValues(t, 5, committed)
	gapStart, gapEnd := ss.GapRange()
	require.EqualValues(t, 3, gapStart)
	require.EqualValues(t, 5, gapEnd)

	entriesR := streamReadAll(t, ss, 0)
	require.EqualValues(t, 4, len(entriesR))

	_, err = ss.PutEntry(entries[3], true)
	require.EqualValues(t, codes.OutOfRange, status.Convert(err).Code())
	err = ss.PutGapEntry(entries[3])
	require.NoError(t, err)
	err = ss.PutGapEntry(entries[4])
	require.NoError(t, err)

	entriesR = streamReadAll(t, ss, 0)
	require.EqualValues(t, 6, len(entriesR), entriesR)
	for idx, entry := range entriesR {
		require.EqualValues(t, entries[idx], entry)
	}

	_, err = ss.PutEntry(entries[10], true)
	require.NoError(t, err)
	for _, e := range entries[4:10] { // Have Gap overlap with Main.
		err = ss.PutGapEntry(e)
		require.NoError(t, err)
	}

	entriesR = streamReadAll(t, ss, 0)
	require.EqualValues(t, 11, len(entriesR), entriesR)
	for idx, entry := range entriesR {
		require.EqualValues(t, entries[idx], entry)
	}
}

func streamReadAll(t *testing.T, ss Stream, entryId int64) []*walleapi.Entry {
	cursor, err := ss.ReadFrom(entryId)
	defer cursor.Close()
	require.NoError(t, err)
	var r []*walleapi.Entry
	for {
		_, ok := cursor.Next()
		if !ok {
			_, ok = cursor.Next()
			require.False(t, ok)
			break
		}
		r = append(r, cursor.Entry())
	}
	return r
}
