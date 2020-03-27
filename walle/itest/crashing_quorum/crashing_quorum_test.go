package crashing_quorum

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
)

func TestCrashingQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rootURI := "/topology/itest"
	wDir := walle.TestTmpDir()

	rootTopology := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.WalleDefaultPort)
	s0 := itest.RunWalle(t, ctx, rootURI, "", rootTopology, wDir, itest.WalleDefaultPort)
	defer s0.Stop(t)
	s1 := itest.RunWalle(t, ctx, rootURI, "", rootTopology, walle.TestTmpDir(), itest.WalleDefaultPort+1)
	defer s1.Stop(t)
	s2 := itest.RunWalle(t, ctx, rootURI, "", rootTopology, walle.TestTmpDir(), itest.WalleDefaultPort+2)
	defer s2.Stop(t)

	rootD, err := wallelib.NewRootDiscovery(ctx, rootURI, rootTopology)
	require.NoError(t, err)
	cli := wallelib.NewClient(ctx, rootD)
	topoMgr := topomgr.NewClient(cli)

	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	var serverIds []string
	for serverId := range topology.Servers {
		serverIds = append(serverIds, serverId)
	}
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   rootURI,
		ServerIds:   serverIds[:2],
	})
	require.NoError(t, err)
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   rootURI,
		ServerIds:   serverIds[:3],
	})
	require.NoError(t, err)
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   "/t1/blast",
		ServerIds:   serverIds,
	})
	require.NoError(t, err)

}
