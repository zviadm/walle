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
	_, c := newMockSystem(ctx, []string{"1", "2", "3"})
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
