package walle

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

// Example simple topology.
var topo3Node = &walleapi.Topology{
	Streams: map[string]*walleapi.StreamTopology{
		"/mock/1": &walleapi.StreamTopology{
			Version:   3,
			ServerIds: []string{"\x00\x01", "\x00\x02", "\x00\x03"},
		},
	},
	Servers: map[string]*walleapi.ServerInfo{
		"\x00\x01": &walleapi.ServerInfo{Address: "localhost1:1001"},
		"\x00\x02": &walleapi.ServerInfo{Address: "localhost2:1001"},
		"\x00\x03": &walleapi.ServerInfo{Address: "localhost3:1001"},
	},
}

func TestProtocolClaimWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, topo3Node, storage.TestTmpDir())

	w, err := wallelib.WaitAndClaim(
		ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w.Close()

	writerStatus, err := c.WriterStatus(ctx, &walleapi.WriterStatusRequest{StreamUri: "/mock/1"})
	require.NoError(t, err)
	require.EqualValues(t, "testhost:1001", writerStatus.WriterAddr)
	require.EqualValues(t, wallelib.LeaseMinimum.Nanoseconds()/time.Millisecond.Nanoseconds(), writerStatus.LeaseMs)
	require.Less(t, int64(0), writerStatus.RemainingLeaseMs)
	require.Greater(t, writerStatus.LeaseMs, writerStatus.RemainingLeaseMs)

	// Make sure heartbeat is working in the background.
	time.Sleep(time.Duration(writerStatus.LeaseMs) * time.Millisecond)
	writerStatus, err = c.WriterStatus(ctx, &walleapi.WriterStatusRequest{StreamUri: "/mock/1"})
	require.NoError(t, err)
	require.Less(t, int64(0), writerStatus.RemainingLeaseMs)
	require.Greater(t, writerStatus.LeaseMs, writerStatus.RemainingLeaseMs)

	e1 := w.PutEntry([]byte("d1"))
	e2 := w.PutEntry([]byte("d2"))
	require.EqualValues(t, e1.Entry.EntryId, 1, "e1: %+v", e1)
	require.EqualValues(t, e2.Entry.EntryId, 2, "e2: %+v", e2)

	zlog.Info("TEST: waiting on e1")
	<-e1.Done()
	require.NoError(t, e1.Err())
	zlog.Info("TEST: waiting on e2")
	<-e2.Done()
	require.NoError(t, e2.Err())

	e3 := w.PutEntry([]byte("d3"))
	require.EqualValues(t, e3.Entry.EntryId, 3, "e3: %+v", e3)
	<-e3.Done()
	require.NoError(t, e3.Err())
	require.True(t, w.IsExclusive())

	// Make sure clean writer transition works.
	w2, err := wallelib.ClaimWriter(ctx, c, "/mock/1", "testhost:1002", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w2.Close()
	require.False(t, w.IsExclusive())
	require.True(t, w2.IsExclusive())

	w2.Close()
	time.Sleep(wallelib.LeaseMinimum*2 + writerTimeoutToResolve)
	writerStatus, err = c.WriterStatus(ctx, &walleapi.WriterStatusRequest{StreamUri: "/mock/1"})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(writerStatus.WriterAddr, "_internal:"), "writerStatus: %s", writerStatus)
	require.LessOrEqual(t, writerStatus.RemainingLeaseMs, int64(0))

	// TODO(zviad): Test at least few edgecases of claim writer reconciliations.
}

func TestProtocolClaimBarrage(t *testing.T) {
	nClaims := 10
	lease := wallelib.LeaseMinimum

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(nClaims)*2*time.Second)
	defer cancel()
	_, c := newMockSystem(ctx, topo3Node, storage.TestTmpDir())

	errChan := make(chan error, nClaims)
	entries := make(chan *walleapi.Entry, nClaims)
	for idx := 0; idx < nClaims; idx++ {
		go func(idx int) (err error) {
			defer func() { errChan <- err }()
			addr := "testhost:" + strconv.Itoa(idx)
			for {
				w, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", addr, lease)
				if err != nil {
					return err
				}
				entry := w.Committed()
				putCtx := w.PutEntry([]byte(strconv.Itoa(idx)))
				<-putCtx.Done()
				w.Close()
				if putCtx.Err() != nil {
					continue // This can happen if WaitAndClaim races.
				}
				zlog.Info("TEST: successful claim ", addr, " read: ", entry.EntryId, " put: ", putCtx.Entry.EntryId)
				entries <- entry
				return nil
			}
		}(idx)
	}
	for idx := 0; idx < nClaims; idx++ {
		err := <-errChan
		require.NoError(t, err)
	}
	for idx := 0; idx < nClaims; idx++ {
		entry := <-entries
		require.EqualValues(t, idx, entry.EntryId)
	}
}

func TestProtocolGapRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, c := newMockSystem(ctx, topo3Node, storage.TestTmpDir())

	w, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w.Close()

	ee := w.PutEntry([]byte("d1"))
	require.EqualValues(t, ee.Entry.EntryId, 1, "ee: %s", ee)
	<-ee.Done()
	require.NoError(t, ee.Err())

	serverIds := topo3Node.Streams["/mock/1"].ServerIds
	m.Toggle(serverIds[0], false)
	ee = w.PutEntry([]byte("d2"))
	<-ee.Done()
	require.NoError(t, ee.Err())
	ee = w.PutEntry([]byte("d3"))
	<-ee.Done()
	require.NoError(t, ee.Err())

	m.Toggle(serverIds[1], false)
	eeD4 := w.PutEntry([]byte("d4"))
	select {
	case <-eeD4.Done():
		t.Fatalf("PutEntry must not have ended: %s", eeD4.Err())
	case <-time.After(10 * time.Millisecond):
	}

	// serverIds[0] will need to create a GAP to succeed with the PutEntry request.
	m.Toggle(serverIds[0], true)
	select {
	case <-eeD4.Done():
	case <-time.After(time.Second):
		t.Fatalf("PutEntry didn't succeed within a timeout")
	}
	require.NoError(t, eeD4.Err())

	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	waitForCommitConvergence(t, ctxTimeout, m, serverIds[0], "/mock/1", eeD4.Entry.EntryId)

	m.Toggle(serverIds[1], true)
	// If client heartbeat is working properly, once 'serverIds[1]' is healthy again, it should force
	// it to catchup with rest of the servers.
	waitForCommitConvergence(t, ctxTimeout, m, serverIds[1], "/mock/1", eeD4.Entry.EntryId)
}

func waitForCommitConvergence(
	t *testing.T,
	ctx context.Context,
	m *mockSystem,
	serverId string,
	streamURI string,
	expectedCommitId int64) {
	s, _ := m.Server(serverId)
	ss, _ := s.s.Stream(streamURI)
	for {
		gapStart, gapEnd := ss.GapRange()
		committed, _ := ss.CommittedEntryId()
		if gapStart >= gapEnd && committed == expectedCommitId {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"timedout waiting for GAP/Catchup Handler: %d -> %d, %d != %d",
				gapStart, gapEnd, committed, expectedCommitId)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
