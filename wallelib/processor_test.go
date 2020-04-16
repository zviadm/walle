package wallelib

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
)

type mockPutter struct {
	Delay time.Duration
	NoOp  bool
	mx    sync.Mutex
	puts  []*walleapi.Entry
}

func (m *mockPutter) Put(ctx context.Context, e *walleapi.Entry) error {
	if m.NoOp {
		return ctx.Err()
	}
	m.mx.Lock()
	defer m.mx.Unlock()
	m.puts = append(m.puts, e)
	select {
	case <-ctx.Done():
	case <-time.After(m.Delay):
	}
	return ctx.Err()
}

func TestProcessorPuts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancelCtx := func(err error) {
		cancel()
	}
	mockP := &mockPutter{}
	p := newProcessor(
		ctx, cancelCtx,
		func() (entryPutter, error) { return mockP, nil },
		1, 4*1024)

	var rs []*PutCtx
	prevEntry := &walleapi.Entry{}
	testData := []byte("test")
	for i := 0; i < 100; i++ {
		r := makePutCtx(prevEntry, prevEntry.WriterId, testData)
		prevEntry = r.Entry
		p.Queue(r)
		rs = append(rs, r)
	}
	for _, r := range rs {
		<-r.Done()
		require.NoError(t, r.Err())
	}
	for idx, r := range rs {
		require.Equal(t, mockP.puts[idx], r.Entry)
	}

	mockP.mx.Lock() // block puts.
	r := makePutCtx(rs[len(rs)-1].Entry, rs[len(rs)-1].Entry.WriterId, testData)
	p.Queue(r)
	select {
	case <-r.Done():
		t.Fatal("put must have blocked")
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	mockP.mx.Unlock() // unblock puts.
	<-r.Done()
	require.Error(t, r.Err())
}

func TestProcessorCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancelCtx := func(err error) {
		cancel()
	}
	mockP := &mockPutter{Delay: 100 * time.Millisecond}
	p := newProcessor(
		ctx, cancelCtx,
		func() (entryPutter, error) { return mockP, nil },
		100, 4*1024)

	var rs []*PutCtx
	prevEntry := &walleapi.Entry{}
	testData := []byte("test")
	for i := 0; i < 100; i++ {
		r := makePutCtx(prevEntry, prevEntry.WriterId, testData)
		prevEntry = r.Entry
		p.Queue(r)
		rs = append(rs, r)
	}
	cancel() // close it before puts are finished.
	for _, r := range rs {
		<-r.Done()
		require.Error(t, r.Err())
	}
}

// BenchmarkProcessor-4 - 334024 - 3722 ns/op - 267 B/op - 3 allocs/op
func BenchmarkProcessor(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	cancelCtx := func(err error) {
		cancel()
	}
	mockP := &mockPutter{NoOp: true}
	p := newProcessor(
		ctx, cancelCtx,
		func() (entryPutter, error) { return mockP, nil },
		maxInFlightPuts, maxInFlightSize)

	rs := make([]*PutCtx, 0, b.N)
	prevEntry := &walleapi.Entry{}
	testData := []byte("test")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := makePutCtx(prevEntry, prevEntry.WriterId, testData)
		prevEntry = r.Entry
		p.Queue(r)
		rs = append(rs, r)
	}
	for _, r := range rs {
		<-r.Done()
		if r.Err() != nil {
			b.Fatal(r.Err())
		}
	}
}
