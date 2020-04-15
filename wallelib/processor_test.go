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
	delay time.Duration
	mx    sync.Mutex
	puts  []*walleapi.Entry
}

func (m *mockPutter) IsPreferred() bool {
	return true
}

func (m *mockPutter) Put(ctx context.Context, e *walleapi.Entry) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.puts = append(m.puts, e)
	select {
	case <-ctx.Done():
	case <-time.After(m.delay):
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
	for i := 0; i < 100; i++ {
		r := &PutCtx{
			Entry: &walleapi.Entry{EntryId: int64(i)},
			done:  make(chan struct{}),
		}
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
	r := &PutCtx{
		Entry: &walleapi.Entry{EntryId: int64(101)},
		done:  make(chan struct{}),
	}
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
	mockP := &mockPutter{delay: 100 * time.Millisecond}
	p := newProcessor(
		ctx, cancelCtx,
		func() (entryPutter, error) { return mockP, nil },
		100, 4*1024)

	var rs []*PutCtx
	for i := 0; i < 100; i++ {
		r := &PutCtx{
			Entry: &walleapi.Entry{EntryId: int64(i)},
			done:  make(chan struct{}),
		}
		p.Queue(r)
		rs = append(rs, r)
	}
	cancel() // close it before puts are finished.
	for _, r := range rs {
		<-r.Done()
		require.Error(t, r.Err())
	}

	r := &PutCtx{
		Entry: &walleapi.Entry{EntryId: int64(101)},
		done:  make(chan struct{}),
	}
	p.Queue(r)
	<-r.Done()
	require.Error(t, r.Err())
}
