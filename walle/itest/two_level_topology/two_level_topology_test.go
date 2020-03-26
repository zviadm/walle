package two_level_topology

import (
	"context"
	"testing"
	"time"

	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/walle/wallelib"
)

var _ = glog.Info

func TestTwoLevelTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wDir := walle.TestTmpDir()
	rootURI := "/topology/itest"
	topologyT1URI := "/topology/t1"
	rootTopology := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.WalleDefaultPort)
	s := itest.RunWalle(t, ctx, rootURI, "", rootTopology, wDir, itest.WalleDefaultPort)
	defer s.Stop(t)

	rootD, err := wallelib.NewRootDiscovery(ctx, rootURI, rootTopology)
	require.NoError(t, err)
	cli := wallelib.NewClient(ctx, rootD)
	topoMgr := topomgr.NewClient(cli)

	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	topology.Version += 1
	topology.Streams[topologyT1URI] = &walleapi.StreamTopology{
		Version:   topology.Version,
		ServerIds: topology.Streams[rootURI].ServerIds,
	}
	_, err = topoMgr.UpdateTopology(ctx, &topomgr_pb.UpdateTopologyRequest{
		TopologyUri: rootURI,
		Topology:    topology,
	})
	require.NoError(t, err)

	// // Need to wait a bit to make sure topology update is propagated to the root
	// // server, so it starts serving the
	// time.Sleep(time.Second)

	// Start regular WALLE server.
	s0 := itest.RunWalle(
		t, ctx, rootURI, topologyT1URI, rootTopology, walle.TestTmpDir(), itest.WalleDefaultPort+1)
	defer s0.Stop(t)

	topology, err = topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: topologyT1URI})
	require.NoError(t, err)
	require.EqualValues(t, 1, len(topology.Servers))
}
