package storage

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
)

func TestStorageOpen(t *testing.T) {
	dbPath := TestTmpDir()
	s, err := Init(dbPath, InitOpts{Create: true})
	require.NoError(t, err)
	err = s.Update("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(t, err)
	s.Close()

	s, err = Init(dbPath, InitOpts{Create: false})
	require.NoError(t, err)
	defer s.Close()
	require.EqualValues(t, s.Streams(false), []string{"/s/1"})
}

func TestStreamStorage(t *testing.T) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true})
	require.NoError(t, err)
	defer s.Close()
	err = s.Update("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(t, err)
	ss, ok := s.Stream("/s/1")
	require.True(t, ok)

	var entries []*walleapi.Entry
	entries = append(entries, Entry0)
	writerId := MakeWriterId()
	_, err = ss.UpdateWriter(writerId, "", 0)
	require.NoError(t, err)
	for idx := 1; idx <= 5; idx++ {
		entry := &walleapi.Entry{
			EntryId:  int64(idx),
			WriterId: writerId,
			Data:     []byte("entry " + strconv.Itoa(idx)),
		}
		entry.ChecksumXX = wallelib.CalculateChecksumXX(entries[idx-1].ChecksumXX, entry.Data)
		entries = append(entries, entry)
	}

	err = ss.PutEntry(entries[1], false)
	require.NoError(t, err)
	committed, _ := ss.CommittedEntryId()
	require.EqualValues(t, 0, committed)
	gapStart, gapEnd := ss.GapRange()
	require.EqualValues(t, 0, gapStart)
	require.EqualValues(t, 0, gapEnd)

	entriesR := streamReadAll(t, ss, 0)
	require.EqualValues(t, 1, len(entriesR)) // entry1 shouldn't be visible yet.
	require.EqualValues(t, 0, entriesR[0].EntryId)

	err = ss.PutEntry(entries[3], false)
	require.EqualValues(t, codes.OutOfRange, status.Convert(err).Code())

	err = ss.PutEntry(entries[3], true)
	require.NoError(t, err)
	committed, _ = ss.CommittedEntryId()
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
	err = ss.PutEntry(entries[5], true)
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
	entry5new.WriterId = MakeWriterId().Encode()
	_, err = ss.UpdateWriter(entry5new.WriterId, "", 0)
	require.NoError(t, err)
	err = ss.PutEntry(entry5new, true)
	require.NoError(t, err)
	entriesR = streamReadAll(t, ss, 5)
	require.Len(t, entriesR, 1)
	require.EqualValues(t, entry5new.WriterId, entriesR[0].WriterId)
}

func TestStreamStorageRaces(t *testing.T) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true})
	require.NoError(t, err)
	defer s.Close()
	err = s.Update("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
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
		err := ss.PutEntry(entry, true)
		require.NoError(t, err)
	}
	cancel()
	for i := 0; i < nReaders; i++ {
		err = <-errC
	}
	require.NoError(t, err)
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

func TestStreamLimits(t *testing.T) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true, MaxLocalStreams: 1})
	require.NoError(t, err)
	defer s.Close()
	longURI := "/" + strings.Repeat("a", streamURIMaxLen-1)
	err = s.Update(longURI, &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(t, err)
	_, ok := s.Stream(longURI)
	require.True(t, ok)

	err = s.Update("/t2", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.Error(t, err) // MaxLocalStreams limitation.
}

func TestStreamOpenClose(t *testing.T) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true, MaxLocalStreams: 1})
	require.NoError(t, err)
	defer s.Close()

	streamURI := "/test1"
	err = s.Update(streamURI, &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId(), "s2"}})
	require.NoError(t, err)
	ss, ok := s.Stream(streamURI)
	require.True(t, ok)
	require.False(t, ss.IsClosed())

	err = s.Update(streamURI, &walleapi.StreamTopology{Version: 2, ServerIds: []string{"s2"}})
	require.NoError(t, err)
	require.True(t, ss.IsClosed())
	_, ok = s.Stream(streamURI)
	require.False(t, ok)

	err = s.Update(streamURI, &walleapi.StreamTopology{Version: 3, ServerIds: []string{s.ServerId(), "s2"}})
	require.NoError(t, err)
	require.True(t, ss.IsClosed())
	ss2, ok := s.Stream(streamURI)
	require.True(t, ok)
	require.False(t, ss2.IsClosed())
}

// BenchmarkPutEntryNoCommit-4 - 249379 - 4531 ns/op - 1.00 cgocalls/op - 256 B/op - 4 allocs/op
func BenchmarkPutEntryNoCommit(b *testing.B) {
	benchmarkPutEntry(b, false)
}

// BenchmarkPutEntryCommitted-4 - 132169 - 10323 ns/op - 2.00 cgocalls/op - 384 B/op - 6 allocs/op
func BenchmarkPutEntryCommitted(b *testing.B) {
	benchmarkPutEntry(b, true)
}

func benchmarkPutEntry(b *testing.B, committed bool) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true, MaxLocalStreams: 1})
	require.NoError(b, err)
	defer s.Close()

	streamURI := "/test1"
	err = s.Update(streamURI, &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(b, err)
	ss, ok := s.Stream(streamURI)
	require.True(b, ok)

	var entries []*walleapi.Entry
	entry := Entry0
	for i := 0; i < b.N+1; i++ {
		checksum := wallelib.CalculateChecksumXX(entry.ChecksumXX, entry.Data)
		entry = &walleapi.Entry{
			EntryId:    entry.EntryId + 1,
			WriterId:   entry.WriterId,
			ChecksumXX: checksum,
			Data:       entry.Data,
		}
		entries = append(entries, entry)
	}
	cgoCalls0 := runtime.NumCgoCall()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err = ss.PutEntry(entries[i], committed)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(runtime.NumCgoCall()-cgoCalls0)/float64(b.N), "cgocalls/op")
}
