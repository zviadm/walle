package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPipelineQueue(t *testing.T) {
	q := newQueue("/test/1")
	for i := 1; i <= 5; i++ {
		_ = q.Queue(&request{
			EntryId:   int64(i),
			Committed: true,
			Entry:     &walleapi.Entry{Data: []byte("test")},
		})
	}
	require.EqualValues(t, 5, len(q.v))
	require.EqualValues(t, 1, q.minId)
	require.EqualValues(t, 5, q.sizeG.Get())
	require.EqualValues(t, 5*len("test"), q.sizeBytesG.Get())
	r, notify := q.PopReady(5, false, nil)
	require.Len(t, r, 5)
	require.EqualValues(t, 6, q.minId)
	require.EqualValues(t, 0, len(q.v))
	require.EqualValues(t, 0, q.sizeG.Get())
	require.EqualValues(t, 0, q.sizeBytesG.Get())
	for i := 10; i >= 6; i-- {
		_ = q.Queue(&request{EntryId: int64(i), Committed: true})
	}
	<-notify
	require.EqualValues(t, 0, q.sizeBytesG.Get())
	_ = q.Queue(&request{
		EntryId: int64(10),
		Entry:   &walleapi.Entry{Data: []byte("test")},
	})
	require.EqualValues(t, len("test"), q.sizeBytesG.Get())

	r, _ = q.PopReady(6, false, r)
	require.Len(t, r, 1)
	r, _ = q.PopReady(10, false, r)
	require.Len(t, r, 4)
	require.EqualValues(t, 0, len(q.v))

	r, notify = q.PopReady(10, false, r)
	require.Len(t, r, 0)
	for i := 12; i <= 15; i++ {
		_ = q.Queue(&request{EntryId: int64(i), Entry: &walleapi.Entry{EntryId: int64(i)}})
	}
	<-notify
	r, notify = q.PopReady(10, false, r)
	require.Len(t, r, 0)

	q.Queue(&request{EntryId: int64(13), Committed: true})
	require.EqualValues(t, 4, len(q.v))
	<-notify
	r, _ = q.PopReady(10, true, r)
	require.Len(t, r, 2)
	require.EqualValues(t, 13, r[0].R.EntryId)
	require.EqualValues(t, 12, r[1].R.EntryId)
}

func TestStreamTimeouts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := storage.TestTmpDir()
	s, err := storage.Init(tmpDir, storage.InitOpts{Create: true, MaxLocalStreams: 2})
	require.NoError(t, err)
	err = s.UpsertStream(
		"/test/1", &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
	require.NoError(t, err)
	ss, ok := s.Stream("/test/1")
	require.True(t, ok)

	q := newStream(ctx, ss, fakeFetch)
	res := q.QueuePut(&walleapi.Entry{EntryId: 2, WriterId: storage.Entry0.WriterId}, false)
	require.NoError(t, res.Err()) // There should be no immediate error.
	select {
	case <-res.Done():
	case <-time.After(QueueMaxTimeout + 100*time.Millisecond):
		t.Fatal("Put must have timed out and errored out!")
	}
	require.Error(t, res.Err())
	require.EqualValues(t, codes.OutOfRange, status.Convert(res.Err()).Code())
}

// BenchmarkQueue-4 - 1117670 - 1526 ns/op - 257 B/op - 4 allocs/op
func BenchmarkQueue(b *testing.B) {
	q := newQueue("/test/1")
	qSize := 1024
	for i := 1; i < qSize; i++ {
		_ = q.Queue(&request{EntryId: int64(i), Committed: true})
	}
	b.ResetTimer()
	b.ReportAllocs()
	var r []queueItem
	for i := 0; i < b.N; i++ {
		_ = q.Queue(&request{EntryId: int64(i + qSize), Committed: true})
		r, _ = q.PopReady(int64(i+1), false, r)
		if len(r) != 1 || r[0].R.EntryId != int64(i+1) {
			b.Fatalf("pop fail: %d - %d %d", i, len(r), r[0].R.EntryId)
		}
	}
}
