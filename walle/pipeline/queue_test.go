package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
)

func TestPipelineQueue(t *testing.T) {
	q := newQueue("/test/1", 1024*1024)
	for i := 1; i <= 5; i++ {
		_, ok := q.Queue(&request{
			EntryId:   int64(i),
			Committed: true,
			Entry:     &walleapi.Entry{Data: []byte("test")},
		})
		require.True(t, ok)
	}
	require.EqualValues(t, 5, len(q.v))
	require.EqualValues(t, 5, q.sizeG.Get())
	require.EqualValues(t, 5*len("test"), q.sizeDataB)
	require.EqualValues(t, 5*len("test"), q.sizeBytesG.Get())
	r, _ := q.PopReady(5, false, nil)
	require.Len(t, r, 5)
	require.EqualValues(t, 0, len(q.v))
	require.EqualValues(t, 0, q.sizeG.Get())
	require.EqualValues(t, 0, q.sizeDataB)
	require.EqualValues(t, 0, q.sizeBytesG.Get())
	for i := 10; i >= 6; i-- {
		_, ok := q.Queue(&request{EntryId: int64(i), Committed: true})
		require.True(t, ok)
	}
	require.EqualValues(t, 0, q.sizeDataB)
	require.EqualValues(t, 0, q.sizeBytesG.Get())
	_, ok := q.Queue(&request{
		EntryId: int64(10),
		Entry:   &walleapi.Entry{Data: []byte("test")},
	})
	require.True(t, ok)
	require.EqualValues(t, len("test"), q.sizeDataB)
	require.EqualValues(t, len("test"), q.sizeBytesG.Get())

	r, _ = q.PopReady(6, false, r)
	require.Len(t, r, 1)
	r, _ = q.PopReady(10, false, r)
	require.Len(t, r, 4)
	require.EqualValues(t, 0, len(q.v))

	for i := 11; i <= 15; i++ {
		_, ok := q.Queue(&request{EntryId: int64(i), Entry: &walleapi.Entry{EntryId: int64(i)}})
		require.True(t, ok)
	}
	q.Queue(&request{EntryId: int64(13), Committed: true})
	require.EqualValues(t, 5, len(q.v))

	// head, _ := q.Peek()
	// require.EqualValues(t, 11, head.EntryId)
	// require.False(t, head.IsReady(9))
	// require.True(t, head.IsReady(10))

	// ii := q.PopReady(9)
	// require.EqualValues(t, 13, ii.R.EntryId)
	// require.EqualValues(t, 13, ii.R.Entry.EntryId)
	// require.True(t, ii.R.Committed)

	// ii = q.PopReady(13)
	// require.EqualValues(t, 11, ii.R.EntryId)
	// require.EqualValues(t, 11, ii.R.Entry.EntryId)
	// require.False(t, ii.R.Committed)
	// require.EqualValues(t, 3, len(q.v))
}

// BenchmarkQueue-4 - 1409140 - 1162 ns/op - 296 B/op - 6 allocs/op
func BenchmarkQueue(b *testing.B) {
	q := newQueue("/test/1", 1024*1024)
	qBuf := maxQueueLen - 1
	for i := 1; i < qBuf; i++ {
		_, ok := q.Queue(&request{EntryId: int64(i), Committed: true})
		require.True(b, ok, "insert fail: %d", i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var r []queueItem
	for i := 0; i < b.N; i++ {
		_, ok := q.Queue(&request{EntryId: int64(i + qBuf), Committed: true})
		if !ok {
			b.Fatalf("insert fail: %d", i)
		}
		r, _ = q.PopReady(int64(i+1), false, r)
		if len(r) != 1 || r[0].R.EntryId != int64(i+1) {
			b.Fatalf("pop fail: %d - %d %d", i, len(r), r[0].R.EntryId)
		}
	}
}
