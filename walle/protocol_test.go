package walle

import (
	"context"
	"strconv"
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
			ServerIds: []string{"s1", "s2", "s3"},
		},
	},
	Servers: map[string]string{"s1": "s1", "s2": "s2", "s3": "s3"},
}

func TestProtocolClaimWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, topoSimple, "/tmp/tt_protocol_basic_new_writer")

	w, _, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w.Close()

	writerStatus, err := c.WriterStatus(ctx, &walleapi.WriterStatusRequest{StreamUri: "/mock/1"})
	require.NoError(t, err)
	require.EqualValues(t, "testhost:1001", writerStatus.WriterAddr)
	require.EqualValues(t, time.Second.Nanoseconds()/time.Millisecond.Nanoseconds(), writerStatus.LeaseMs)
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
	require.True(t, w.IsWriter())

	// Make sure clean writer transition works.
	w2, _, err := wallelib.ClaimWriter(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w2.Close()
	require.False(t, w.IsWriter())
	require.True(t, w2.IsWriter())

	// TODO(zviad): Test at least few edgecases of claim writer reconciliations.
}

func TestProtocolClaimBarrage(t *testing.T) {
	nClaims := 10
	lease := wallelib.LeaseMinimum

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(nClaims)*time.Second)
	defer cancel()
	_, c := newMockSystem(ctx, topoSimple, "/tmp/tt_protocol_basic_new_writer")

	errChan := make(chan error, nClaims)
	entries := make(chan *walleapi.Entry, nClaims)
	for idx := 0; idx < nClaims; idx++ {
		go func(idx int) (err error) {
			defer func() { errChan <- err }()
			w, entry, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:"+strconv.Itoa(idx), lease)
			if err != nil {
				return err
			}
			defer w.Close()
			glog.Info("Successful claim ", entry.EntryId, idx)
			entries <- entry
			w.PutEntry([]byte(strconv.Itoa(idx)))
			return nil
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
	defer w.Close()

	ee, errC := w.PutEntry([]byte("d1"))
	require.EqualValues(t, ee.EntryId, 1, "ee: %v", ee)
	err = <-errC
	require.NoError(t, err)

	m.Toggle("s1", false)
	_, errC = w.PutEntry([]byte("d2"))
	err = <-errC
	require.NoError(t, err)
	_, errC = w.PutEntry([]byte("d3"))
	err = <-errC
	require.NoError(t, err)

	m.Toggle("s2", false)
	eeD4, errC := w.PutEntry([]byte("d4"))
	select {
	case err = <-errC:
		t.Fatalf("PutEntry must not have succeeded: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	// s1 will need to create a GAP to succeed with the PutEntry request.
	m.Toggle("s1", true)
	select {
	case err = <-errC:
	case <-time.After(time.Second):
		t.Fatalf("PutEntry didn't succeed within a timeout")
	}
	require.NoError(t, err)

	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	waitForCommitConvergence(t, ctxTimeout, m, "s1", "/mock/1", eeD4.EntryId)

	m.Toggle("s2", true)
	// If client heartbeat is working properly, once 's2' is healthy again, it should force
	// it to catchup with rest of the servers.
	waitForCommitConvergence(t, ctxTimeout, m, "s2", "/mock/1", eeD4.EntryId)
}

func waitForCommitConvergence(
	t *testing.T,
	ctx context.Context,
	m *mockSystem,
	serverId string,
	streamURI string,
	expectedCommitId int64) {
	s1, _ := m.Server(serverId)
	ss1, _ := s1.s.Stream(streamURI, false)
	for {
		noGap, committed, notify := ss1.CommittedEntryIds()
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
