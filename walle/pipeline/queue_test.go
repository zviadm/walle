package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
)

func TestPipelineQueue(t *testing.T) {
	q := newQueue(1024 * 1024)
	for i := 1; i <= 5; i++ {
		_ = q.Queue(&Request{EntryId: int64(i), Committed: true})
	}
	for i := 1; i <= 5; i++ {
		ii := q.PopReady(5)
		require.EqualValues(t, i, ii.R.EntryId)
	}
	require.EqualValues(t, 0, len(q.v))
	for i := 5; i >= 1; i-- {
		_ = q.Queue(&Request{EntryId: int64(i), Committed: true})
	}
	for i := 1; i <= 5; i++ {
		ii := q.PopReady(5)
		require.EqualValues(t, i, ii.R.EntryId)
	}
	require.EqualValues(t, 0, len(q.v))

	for i := 11; i <= 15; i++ {
		_ = q.Queue(&Request{EntryId: int64(i), Entry: &walleapi.Entry{EntryId: int64(i)}})
	}
	q.Queue(&Request{EntryId: int64(13), Committed: true})
	require.EqualValues(t, 5, len(q.v))

	head, _ := q.Peek()
	require.EqualValues(t, 11, head.EntryId)
	require.False(t, head.IsReady(9))
	require.True(t, head.IsReady(10))

	ii := q.PopReady(9)
	require.EqualValues(t, 13, ii.R.EntryId)
	require.EqualValues(t, 13, ii.R.Entry.EntryId)
	require.True(t, ii.R.Committed)

	ii = q.PopReady(13)
	require.EqualValues(t, 11, ii.R.EntryId)
	require.EqualValues(t, 11, ii.R.Entry.EntryId)
	require.False(t, ii.R.Committed)
	require.EqualValues(t, 3, len(q.v))
}

func BenchmarkQueue(b *testing.B) {
	q := newQueue(1024 * 1024 * 1024)
	for i := 0; i < 1024; i++ {
		_ = q.Queue(&Request{EntryId: int64(i), Committed: true})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Queue(&Request{EntryId: int64(i + 1024), Committed: true})
		_ = q.PopReady(int64(i))
	}
}
