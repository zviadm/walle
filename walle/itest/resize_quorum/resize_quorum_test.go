package resize_quorum

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/tt/servicelib"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/walle/wallelib"
	"github.com/zviadm/zlog"
)

func TestResizeQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rootURI := "/topology/itest"
	wDir := walle.TestTmpDir()

	rootTopology := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.WalleDefaultPort)
	s := itest.RunWalle(t, ctx, rootURI, "", rootTopology, wDir, itest.WalleDefaultPort)
	defer s.Stop(t)

	rootD, err := wallelib.NewRootDiscovery(ctx, rootURI, rootTopology)
	require.NoError(t, err)
	cli := wallelib.NewClient(ctx, rootD)
	topoMgr := topomgr.NewClient(cli)
	services := []*servicelib.Service{s}
	nTotal := 5
	for idx := 1; idx < nTotal; idx++ {
		s := expandTopology(t, ctx, topoMgr, rootURI, itest.WalleDefaultPort+idx)
		defer s.Stop(t)
		services = append(services, s)
	}
	for _, s := range services[:nTotal-1] {
		shrinkTopology(t, ctx, s, topoMgr, cli, rootURI)
	}
}

func expandTopology(
	t *testing.T,
	ctx context.Context,
	topoMgr topomgr_pb.TopoManagerClient,
	rootURI string,
	port int) *servicelib.Service {

	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	s := itest.RunWalle(t, ctx, rootURI, "", topology, walle.TestTmpDir(), port)

	topology, err = topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	serverId := serverIdsDiff(t, topology.Servers, topology.Streams[rootURI].ServerIds)
	serverIds := append(topology.Streams[rootURI].ServerIds, serverId)
	zlog.Info("--- expanding to: ", serverAddrs(topology.Servers, serverIds))
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   rootURI,
		ServerIds:   serverIds,
	})
	require.NoError(t, err)
	return s
}

func serverIdsDiff(t *testing.T, serverIdsAll map[string]*walleapi.ServerInfo, serverIds []string) string {
	for serverId := range serverIdsAll {
		found := false
		for _, s := range serverIds {
			if s == serverId {
				found = true
				break
			}
		}
		if !found {
			return serverId
		}
	}
	panic(fmt.Sprintf("can't diff: %s - %s", serverIdsAll, serverIds))
}

func shrinkTopology(
	t *testing.T,
	ctx context.Context,
	s *servicelib.Service,
	topoMgr topomgr_pb.TopoManagerClient,
	cli wallelib.BasicClient,
	rootURI string) {
	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)

	serverIds := topology.Streams[rootURI].ServerIds[1:]
	zlog.Info("--- shrinking to: ", serverAddrs(topology.Servers, serverIds))
	resp, err := topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   rootURI,
		ServerIds:   serverIds,
	})
	require.NoError(t, err)

	if len(serverIds) == 1 {
		// When shrinking from 2->1 node, we need to make sure topology updates are propagated
		// before shutting down the removed node. Otherwise quorum will be lost and state won't
		// converge.
		streamC, err := cli.ForStream(rootURI)
		require.NoError(t, err)
		for {
			wStatus, err := streamC.WriterStatus(ctx, &walleapi.WriterStatusRequest{StreamUri: rootURI})
			require.NoError(t, err)
			if wStatus.StreamVersion >= resp.StreamVersion {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	s.Stop(t)
}

func serverAddrs(servers map[string]*walleapi.ServerInfo, serverIds []string) []string {
	r := make([]string, len(serverIds))
	for idx, serverId := range serverIds {
		r[idx] = servers[serverId].Address
	}
	return r
}
