package walle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
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

	ss, ok := s.Stream("/s/1", true)
	require.True(t, ok)
	entry := &walleapi.Entry{
		EntryId:  1,
		WriterId: entry0.WriterId,
		Data:     []byte("entry 1"),
	}
	entry.ChecksumMd5 = wallelib.CalculateChecksumMd5(entry0.ChecksumMd5, entry.Data)
	ok = ss.PutEntry(entry, true)
	require.True(t, ok)
	cursor := ss.ReadFrom(0)
	defer cursor.Close()
	var idx int64 = 0
	for ; ; idx++ {
		v, ok := cursor.Next()
		if !ok {
			_, ok = cursor.Next()
			require.False(t, ok)
			break
		}
		require.EqualValues(t, idx, v.EntryId)
	}
	require.EqualValues(t, entry.EntryId+1, idx)
}
