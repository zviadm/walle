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
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

func TestResizeQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rootURI := "/topology/itest"
	wDir := storage.TestTmpDir()

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
	s := itest.RunWalle(t, ctx, rootURI, "", topology, storage.TestTmpDir(), port)

	topology, err = topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	serverId := serverIdsDiff(t, topology.Servers, topology.Streams[rootURI].ServerIds)
	serverIds := append(topology.Streams[rootURI].ServerIds, serverId)
	zlog.Info("TEST: --- expanding to: ", serverAddrs(topology.Servers, serverIds))
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
	cli wallelib.Client,
	rootURI string) {
	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)

	serverIds := topology.Streams[rootURI].ServerIds[1:]
	zlog.Info("TEST: --- shrinking to: ", serverAddrs(topology.Servers, serverIds))
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   rootURI,
		ServerIds:   serverIds,
	})
	require.NoError(t, err)
	s.Stop(t)
}

func serverAddrs(servers map[string]*walleapi.ServerInfo, serverIds []string) []string {
	r := make([]string, len(serverIds))
	for idx, serverId := range serverIds {
		r[idx] = servers[serverId].Address
	}
	return r
}
