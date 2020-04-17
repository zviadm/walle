package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
)

func TestProtocolGapRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, c := newMockSystem(ctx, cluster3Node, storage.TestTmpDir())

	w, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w.Close()

	ee := w.PutEntry([]byte("d1"))
	require.EqualValues(t, ee.Entry.EntryId, 1, "ee: %s", ee)
	<-ee.Done()
	require.NoError(t, ee.Err())

	serverIds := cluster3Node.Streams["/mock/1"].ServerIds
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
	waitForCommitConvergence(ctxTimeout, t, m, serverIds[0], "/mock/1", eeD4.Entry.EntryId)

	m.Toggle(serverIds[1], true)
	// If client heartbeat is working properly, once 'serverIds[1]' is healthy again, it should force
	// it to catchup with rest of the servers.
	waitForCommitConvergence(ctxTimeout, t, m, serverIds[1], "/mock/1", eeD4.Entry.EntryId)

	for sIdx := 0; sIdx < 2; sIdx++ {
		cc, err := c.ForServer(serverIds[sIdx])
		require.NoError(t, err)
		entries, err := readEntriesAll(ctx, cc, &walle_pb.ReadEntriesRequest{
			ServerId:      serverIds[sIdx],
			StreamUri:     "/mock/1",
			StreamVersion: cluster3Node.Streams["/mock/1"].Version,
			FromServerId:  serverIds[sIdx],

			StartEntryId: 0,
			EndEntryId:   eeD4.Entry.EntryId + 1,
		})
		require.NoError(t, err)
		for idx, entry := range entries {
			require.EqualValues(t, idx, entry.EntryId)
		}
	}
}

func waitForCommitConvergence(
	ctx context.Context,
	t *testing.T,
	m *mockSystem,
	serverId string,
	streamURI string,
	expectedCommitId int64) {
	s, _ := m.Server(serverId)
	ss, _ := s.s.Stream(streamURI)
	for {
		gapStart, gapEnd := ss.GapRange()
		committed := ss.CommittedId()
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
