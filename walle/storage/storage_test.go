package storage

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
)

func TestStorageOpen(t *testing.T) {
	dbPath := TestTmpDir()
	s, err := Init(dbPath, InitOpts{Create: true})
	require.NoError(t, err)
	err = s.CrUpdateStream("/s/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(t, err)
	s.Close()

	s, err = Init(dbPath, InitOpts{Create: false})
	require.NoError(t, err)
	defer s.Close()
	require.EqualValues(t, s.LocalStreams(), []string{"/s/1"})

	ss, ok := s.Stream("/s/1")
	require.True(t, ok)
	require.EqualValues(t, 1, ss.Topology().Version)
}

func TestStreamLimits(t *testing.T) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true, MaxLocalStreams: 1})
	require.NoError(t, err)
	defer s.Close()
	longURI := "/" + strings.Repeat("a", streamURIMaxLen-1)
	err = s.CrUpdateStream(longURI, &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(t, err)
	_, ok := s.Stream(longURI)
	require.True(t, ok)

	err = s.CrUpdateStream("/t2", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.Error(t, err) // MaxLocalStreams limitation.
}

func TestStreamOpenClose(t *testing.T) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true, MaxLocalStreams: 1})
	require.NoError(t, err)
	defer s.Close()

	streamURI := "/test1"
	err = s.CrUpdateStream(streamURI, &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId(), "s2"}})
	require.NoError(t, err)
	ss, ok := s.Stream(streamURI)
	require.True(t, ok)
	require.False(t, ss.IsClosed())

	err = s.CrUpdateStream(streamURI, &walleapi.StreamTopology{Version: 2, ServerIds: []string{"s2"}})
	require.NoError(t, err)
	require.True(t, ss.IsClosed())
	_, ok = s.Stream(streamURI)
	require.False(t, ok)

	err = s.CrUpdateStream(streamURI, &walleapi.StreamTopology{Version: 3, ServerIds: []string{s.ServerId(), "s2"}})
	require.NoError(t, err)
	require.True(t, ss.IsClosed())
	ss2, ok := s.Stream(streamURI)
	require.True(t, ok)
	require.False(t, ss2.IsClosed())
}

// BenchmarkPutEntryNoCommit-4 - 1.00 cgocalls/op - 0 B/op - 0 allocs/op
func BenchmarkPutEntryNoCommit(b *testing.B) {
	benchmarkPutEntry(b, false)
}

// BenchmarkPutEntryCommitted-4 - 2.00 cgocalls/op - 96 B/op - 1 allocs/op
func BenchmarkPutEntryCommitted(b *testing.B) {
	benchmarkPutEntry(b, true)
}

func benchmarkPutEntry(b *testing.B, committed bool) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true, MaxLocalStreams: 1})
	require.NoError(b, err)
	defer s.Close()

	streamURI := "/test1"
	err = s.CrUpdateStream(streamURI, &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
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
		forceCommit := i%(maxUncommittedEntries/2) == 0
		_, err = ss.PutEntry(entries[i], committed || forceCommit)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(runtime.NumCgoCall()-cgoCalls0)/float64(b.N), "cgocalls/op")
}
