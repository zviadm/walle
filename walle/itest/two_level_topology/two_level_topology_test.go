package two_level_topology

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

var _ = zlog.Info

func TestTwoLevelTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wDir := storage.TestTmpDir()
	rootURI := topomgr.Prefix + "itest"
	topologyT1URI := topomgr.Prefix + "t1"
	t1URIs := []string{"/t1/0", "/t1/1", "/t1/2", "/t1/3"}

	rootPb := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.WalleDefaultPort)
	s := itest.RunWalle(t, ctx, rootURI, "", rootPb, wDir, itest.WalleDefaultPort)
	defer s.Stop(t)

	cli, err := wallelib.NewClientFromRootPb(ctx, rootPb, "")
	require.NoError(t, err)
	topoMgr := topomgr.NewClient(cli)

	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   topologyT1URI,
		ServerIds:   topology.Streams[rootURI].ServerIds,
	})
	require.NoError(t, err)

	// Start regular WALLE servers serving in `t1` cluster.
	nT1 := 3
	for i := 1; i <= nT1; i++ {
		sT1 := itest.RunWalle(
			t, ctx, rootURI, topologyT1URI, rootPb, storage.TestTmpDir(), itest.WalleDefaultPort+i)
		defer sT1.Kill(t)
	}

	topology, err = topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: topologyT1URI})
	require.NoError(t, err)
	for _, t1URI := range t1URIs {
		_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
			TopologyUri: topologyT1URI,
			StreamUri:   t1URI,
			ServerIds:   itest.ServerIdsSlice(topology.Servers),
		})
		require.NoError(t, err)
	}
}
