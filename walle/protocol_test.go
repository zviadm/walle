package walle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/walle/wallelib"
)

func TestProtocolBasicNewWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, []string{"s1", "s2", "s3"})
	w, err := wallelib.ClaimWriter(ctx, c, "/mock/1", time.Second)
	require.NoError(t, err)
	defer w.Close()

	e1, c1 := w.PutEntry([]byte("d1"))
	e2, c2 := w.PutEntry([]byte("d2"))
	require.EqualValues(t, e1.EntryId, 1, "e1: %+v", e1)
	require.EqualValues(t, e2.EntryId, 2, "e2: %+v", e2)

	err = <-c2
	require.NoError(t, err)
	select {
	case err := <-c1:
		require.NoError(t, err)
	default:
		t.Fatalf("c1 must have been ready since c2 was ready")
	}

	e3, c3 := w.PutEntry([]byte("d3"))
	require.EqualValues(t, e3.EntryId, 3, "e3: %+v", e3)
	err = <-c3
	require.NoError(t, err)

	// for _, serverId := range []string{"1", "2", "3"} {
	// 	resp, err := c.Preferred("/mock/1").LastEntry(
	// 		ctx, &walle_pb.LastEntryRequest{TargetServerId: serverId, StreamUri: "/mock/1"})
	// 	require.NoError(t, err)
	// 	require.EqualValues(t, resp.Entries[0].EntryId, 2)
	// }
}

func TestProtocolBasicGapCatchupRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, c := newMockSystem(ctx, []string{"s1", "s2", "s3"})
	w, err := wallelib.ClaimWriter(ctx, c, "/mock/1", time.Second)
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
	_, errC = w.PutEntry([]byte("d4"))
	select {
	case err = <-errC:
		t.Fatalf("PutEntry must not have succeeded: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	// server1 can only succeed if it can successfully catch up to server3.
	m.Toggle("s1", true)
	select {
	case err = <-errC:
	case <-time.After(time.Second):
		t.Fatalf("PutEntry didn't succeed within a timeout")
	}
	require.NoError(t, err)

	s1, _ := m.Server("s1")
	ss1, _ := s1.s.Stream("/mock/1")
	timeoutDeadline := time.Now().Add(3 * time.Second)
	for {
		noGap, committed, maxCommitted := ss1.CommittedEntryIds()
		if noGap == committed && committed == maxCommitted {
			break
		}
		if time.Now().After(timeoutDeadline) {
			t.Fatalf("timedout waiting for GAP/Catchup Handler: %d -> %d -> %d", noGap, committed, maxCommitted)
		}
		time.Sleep(time.Second) // TODO(zviad): make this smaller and notify gap handler.
	}
}
