package walle

import (
	"context"
	"testing"
	"time"

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
	w, err := wallelib.ClaimWriter(ctx, c, "/mock/1", time.Second, "testhost:1001")
	require.NoError(t, err)
	defer w.Close()

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

	// Make sure clean writer transition works.
	w2, err := wallelib.ClaimWriter(ctx, c, "/mock/1", time.Second, "testhost:1001")
	require.NoError(t, err)
	defer w2.Close()

	// TODO(zviad): Test at least few edgecases of claim writer reconciliations.
}

func TestProtocolGapRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, c := newMockSystem(ctx, topoSimple, TestTmpDir())
	w, err := wallelib.ClaimWriter(ctx, c, "/mock/1", time.Second, "testhost:1001")
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
