package walle

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
)

// Example simple topology.
var topoSimple = &walleapi.Topology{
	Streams: map[string]*walleapi.StreamTopology{
		"/mock/1": &walleapi.StreamTopology{
			Version:   3,
			ServerIds: []string{"\x00\x01", "\x00\x02", "\x00\x03"},
		},
	},
	Servers: map[string]string{
		"\x00\x01": "localhost1:1001",
		"\x00\x02": "localhost2:1001",
		"\x00\x03": "localhost3:1001",
	},
}

func TestProtocolClaimWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, topoSimple, TestTmpDir())

	w, _, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w.Close(false)

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

	e1, c1 := w.PutEntry([]byte("d1"))
	e2, c2 := w.PutEntry([]byte("d2"))
	require.EqualValues(t, e1.EntryId, 1, "e1: %+v", e1)
	require.EqualValues(t, e2.EntryId, 2, "e2: %+v", e2)

	err = <-c2
	require.NoError(t, err)
	err = <-c1
	require.NoError(t, err)

	e3, c3 := w.PutEntry([]byte("d3"))
	require.EqualValues(t, e3.EntryId, 3, "e3: %+v", e3)
	err = <-c3
	require.NoError(t, err)
	state, _ := w.WriterState()
	require.Equal(t, wallelib.Exclusive, state)

	// Make sure clean writer transition works.
	w2, _, err := wallelib.ClaimWriter(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w2.Close(false)
	state, _ = w.WriterState()
	require.NotEqual(t, wallelib.Exclusive, state)
	state2, _ := w2.WriterState()
	require.Equal(t, wallelib.Exclusive, state2)

	w2.Close(false)
	time.Sleep(2 * writerTimeoutToResolve)
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
	_, c := newMockSystem(ctx, topoSimple, TestTmpDir())

	errChan := make(chan error, nClaims)
	entries := make(chan *walleapi.Entry, nClaims)
	for idx := 0; idx < nClaims; idx++ {
		go func(idx int) (err error) {
			defer func() { errChan <- err }()
			addr := "testhost:" + strconv.Itoa(idx)
			for {
				w, entry, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", addr, lease)
				if err != nil {
					return err
				}
				defer w.Close(true)
				_, errC := w.PutEntry([]byte(strconv.Itoa(idx)))
				err = <-errC
				if err != nil {
					continue // This can happen if WaitAndClaim races.
				}
				glog.Info("Successful claim ", addr, " ", entry.EntryId)
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
	m, c := newMockSystem(ctx, topoSimple, TestTmpDir())

	w, _, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w.Close(false)

	ee, errC := w.PutEntry([]byte("d1"))
	require.EqualValues(t, ee.EntryId, 1, "ee: %s", ee)
	err = <-errC
	require.NoError(t, err)

	serverIds := topoSimple.Streams["/mock/1"].ServerIds
	m.Toggle(serverIds[0], false)
	_, errC = w.PutEntry([]byte("d2"))
	err = <-errC
	require.NoError(t, err)
	_, errC = w.PutEntry([]byte("d3"))
	err = <-errC
	require.NoError(t, err)

	m.Toggle(serverIds[1], false)
	eeD4, errC := w.PutEntry([]byte("d4"))
	select {
	case err = <-errC:
		t.Fatalf("PutEntry must not have succeeded: %s", err)
	case <-time.After(10 * time.Millisecond):
	}

	// serverIds[0] will need to create a GAP to succeed with the PutEntry request.
	m.Toggle(serverIds[0], true)
	select {
	case err = <-errC:
	case <-time.After(time.Second):
		t.Fatalf("PutEntry didn't succeed within a timeout")
	}
	require.NoError(t, err)

	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	waitForCommitConvergence(t, ctxTimeout, m, serverIds[0], "/mock/1", eeD4.EntryId)

	m.Toggle(serverIds[1], true)
	// If client heartbeat is working properly, once 'serverIds[1]' is healthy again, it should force
	// it to catchup with rest of the servers.
	waitForCommitConvergence(t, ctxTimeout, m, serverIds[1], "/mock/1", eeD4.EntryId)
}

func waitForCommitConvergence(
	t *testing.T,
	ctx context.Context,
	m *mockSystem,
	serverId string,
	streamURI string,
	expectedCommitId int64) {
	s, _ := m.Server(serverId)
	ss, _ := s.s.Stream(streamURI, false)
	for {
		noGap, committed, notify := ss.CommittedEntryIds()
		if noGap == committed && committed == expectedCommitId {
			break
		}
		select {
		case <-notify:
		case <-ctx.Done():
			t.Fatalf("timedout waiting for GAP/Catchup Handler: %d -> %d != %d", noGap, committed, expectedCommitId)
		}
	}
}
