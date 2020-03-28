package two_level_topology

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

var _ = zlog.Info

func TestTwoLevelTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wDir := walle.TestTmpDir()
	rootURI := "/topology/itest"
	topologyT1URI := "/topology/t1"
	t1URIs := []string{"/t1/0", "/t1/1", "/t1/2", "/t1/3"}

	rootTopology := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.WalleDefaultPort)
	s := itest.RunWalle(t, ctx, rootURI, "", rootTopology, wDir, itest.WalleDefaultPort)
	defer s.Stop(t)

	rootD, err := wallelib.NewRootDiscovery(ctx, rootURI, rootTopology)
	require.NoError(t, err)
	cli := wallelib.NewClient(ctx, rootD)
	topoMgr := topomgr.NewClient(cli)

	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   topologyT1URI,
		ServerIds:   topology.Streams[rootURI].ServerIds,
	})
	require.NoError(t, err)

	// Start regular WALLE servers serving in `/topology/t1`.
	nT1 := 3
	for i := 0; i < nT1; i++ {
		sT1 := itest.RunWalle(
			t, ctx, rootURI, topologyT1URI, rootTopology, walle.TestTmpDir(), itest.WalleDefaultPort+i+1)
		defer sT1.Kill(t)
	}

	topology, err = topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: topologyT1URI})
	require.NoError(t, err)
	var serverIds []string
	for serverId := range topology.Servers {
		serverIds = append(serverIds, serverId)
	}
	require.EqualValues(t, nT1, len(serverIds))
	for _, t1URI := range t1URIs {
		_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
			TopologyUri: topologyT1URI,
			StreamUri:   t1URI,
			ServerIds:   serverIds,
		})
		require.NoError(t, err)
	}
}
