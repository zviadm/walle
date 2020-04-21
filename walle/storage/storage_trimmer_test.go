package storage

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zviadm/walle/proto/walleapi"
)

func TestStorageTrimming(t *testing.T) {
	s, err := Init(TestTmpDir(), InitOpts{Create: true})
	require.NoError(t, err)
	defer s.Close()

	uri := "/test/1"
	err = s.CrUpdateStream(uri, &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(t, err)
	ss, _ := s.Stream(uri)
	entry := Entry0
	for idx := 1; idx < 100; idx++ {
		entry = makeEntry(entry, []byte("entry"+strconv.Itoa(idx)))
		_, err = ss.PutEntry(entry, true)
		require.NoError(t, err)
	}

	err = s.CrUpdateStream(uri, &walleapi.StreamTopology{
		Version:      2,
		ServerIds:    []string{s.ServerId()},
		FirstEntryId: entry.EntryId,
		FirstEntryXX: entry.ChecksumXX,
	})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond) // make sure trimming goes through.
	entries := streamReadAll(t, ss, 0)
	require.Len(t, entries, 1)
	require.EqualValues(t, entry, entries[0])

	var entry106 *walleapi.Entry
	for idx := 100; idx < 110; idx++ {
		entry = makeEntry(entry, []byte("entry"+strconv.Itoa(idx)))
		if idx > 105 { //Create GAP: [100..105]
			_, err = ss.PutEntry(entry, true)
			require.NoError(t, err)
		}
		if idx == 106 {
			entry106 = entry
		}
	}
	gapStart, gapEnd := ss.GapRange()
	require.EqualValues(t, 100, gapStart)
	require.EqualValues(t, 106, gapEnd)

	err = s.CrUpdateStream(uri, &walleapi.StreamTopology{
		Version:      3,
		ServerIds:    []string{s.ServerId()},
		FirstEntryId: entry106.EntryId,
		FirstEntryXX: entry106.ChecksumXX,
	})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond) // make sure trimming goes through.

	gapStart, gapEnd = ss.GapRange()
	require.EqualValues(t, 0, gapStart)
	require.EqualValues(t, 0, gapEnd)
	entries = streamReadAll(t, ss, 0)
	require.Len(t, entries, 4)
	require.EqualValues(t, entry106, entries[0])
}
