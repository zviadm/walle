package stream_blast

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/tt/servicelib"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

var _ = zlog.Info

func TestStreamBlast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wDir := storage.TestTmpDir()
	rootURI := "/topology/itest"
	blastURI := "/t1/blast"

	rootTopology := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.WalleDefaultPort)
	s := make([]*servicelib.Service, 3)
	s[0] = itest.RunWalle(t, ctx, rootURI, "", rootTopology, wDir, itest.WalleDefaultPort)
	defer s[0].Stop(t)
	for i := 1; i <= 2; i++ {
		s[i] = itest.RunWalle(
			t, ctx, rootURI, "", rootTopology, storage.TestTmpDir(), itest.WalleDefaultPort+i)
		defer s[i].Kill(t)
	}

	rootD, err := wallelib.NewRootDiscovery(ctx, rootURI, rootTopology)
	require.NoError(t, err)
	cli := wallelib.NewClient(ctx, rootD)
	topoMgr := topomgr.NewClient(cli)

	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   blastURI,
		ServerIds:   itest.ServerIdsSlice(topology.Servers),
	})
	require.NoError(t, err)

	w, _, err := wallelib.WaitAndClaim(
		ctx, cli, blastURI, "blastwriter:1001", time.Second)
	require.NoError(t, err)
	defer w.Close()

	// Test with full quorum.
	itest.PutBatch(t, w, 3000, 10)
	itest.PutBatch(t, w, 3000, 100)
	itest.PutBatch(t, w, 3000, 1000)

	// Test with one node down.
	defer servicelib.IptablesClearAll(t)
	servicelib.IptablesBlockPort(t, itest.WalleDefaultPort+2)
	s[2].Kill(t)
	itest.PutBatch(t, w, 3000, 10)
	itest.PutBatch(t, w, 3000, 100)
	itest.PutBatch(t, w, 3000, 1000)
}
