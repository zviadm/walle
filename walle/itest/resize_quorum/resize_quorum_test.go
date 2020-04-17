package resize_quorum

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/tt/servicelib"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/zlog"
)

func TestResizeQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer servicelib.KillAll()

	services, rootPb, cli := itest.SetupRootNodes(ctx, t, 1)
	topoMgr := topomgr.NewClient(cli)
	nTotal := 5
	for idx := 1; idx < nTotal; idx++ {
		s := expandTopology(ctx, t, topoMgr, rootPb.RootUri, itest.RootDefaultPort+idx)
		services = append(services, s)
	}
	for _, s := range services[:nTotal-1] {
		shrinkTopology(ctx, t, s, topoMgr, rootPb.RootUri)
	}
}

func expandTopology(
	ctx context.Context,
	t *testing.T,
	topoMgr topomgr_pb.TopoManagerClient,
	rootURI string,
	port int) *servicelib.Service {

	rootPb, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{ClusterUri: rootURI})
	require.NoError(t, err)
	s, err := itest.RunWalle(ctx, rootPb, rootPb.RootUri, storage.TestTmpDir(), port)
	require.NoError(t, err)

	rootPb, err = topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{ClusterUri: rootURI})
	require.NoError(t, err)
	serverId := serverIdsDiff(t, rootPb.Servers, rootPb.Streams[rootURI].ServerIds)
	serverIds := append(rootPb.Streams[rootURI].ServerIds, serverId)
	zlog.Info("TEST: --- expanding to: ", serverAddrs(rootPb.Servers, serverIds))
	_, err = topoMgr.CrUpdateStream(ctx, &topomgr_pb.CrUpdateStreamRequest{
		ClusterUri: rootURI,
		StreamUri:  rootURI,
		ServerIds:  serverIds,
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
	ctx context.Context,
	t *testing.T,
	s *servicelib.Service,
	topoMgr topomgr_pb.TopoManagerClient,
	rootURI string) {
	rootPb, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{ClusterUri: rootURI})
	require.NoError(t, err)

	serverIds := rootPb.Streams[rootURI].ServerIds[1:]
	zlog.Info("TEST: --- shrinking to: ", serverAddrs(rootPb.Servers, serverIds))
	_, err = topoMgr.CrUpdateStream(ctx, &topomgr_pb.CrUpdateStreamRequest{
		ClusterUri: rootURI,
		StreamUri:  rootURI,
		ServerIds:  serverIds,
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, s.Stop()) // Make sure graceful stop is working.
}

func serverAddrs(servers map[string]*walleapi.ServerInfo, serverIds []string) []string {
	r := make([]string, len(serverIds))
	for idx, serverId := range serverIds {
		r[idx] = servers[serverId].Address
	}
	return r
}
