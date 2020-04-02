package itest

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/tt/servicelib"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
)

func SetupRootNodes(
	t *testing.T, ctx context.Context, rootN int) (
	s []*servicelib.Service, rootPb *walleapi.Topology, rootCli wallelib.Client) {
	rootURI := topomgr.Prefix + "itest"
	wDir0 := storage.TestTmpDir()
	rootPb = BootstrapDeployment(t, ctx, rootURI, wDir0, RootDefaultPort)
	s = make([]*servicelib.Service, rootN)
	mx := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(s))
	for idx := range s {
		go func(idx int) {
			defer wg.Done()
			wDir := wDir0
			if idx > 0 {
				wDir = storage.TestTmpDir()
			}
			ss := RunWalle(t, ctx, rootPb, "", wDir, RootDefaultPort+idx)
			mx.Lock()
			defer mx.Unlock()
			s[idx] = ss
		}(idx)
	}
	wg.Wait()

	var err error
	rootCli, err = wallelib.NewClientFromRootPb(ctx, rootPb, "")
	require.NoError(t, err)
	topoMgr := topomgr.NewClient(rootCli)
	topology, err := topoMgr.FetchTopology(
		ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	serverIds := ServerIdsSlice(topology.Servers)
	require.Len(t, serverIds, rootN)
	require.Len(t, topology.Streams[rootURI].ServerIds, 1)
	for idx, serverId := range serverIds {
		if serverId != topology.Streams[rootURI].ServerIds[0] {
			continue
		}
		serverIds[0], serverIds[idx] = serverIds[idx], serverIds[0]
		break
	}
	for i := 1; i < rootN; i++ {
		_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
			TopologyUri: rootURI,
			StreamUri:   rootURI,
			ServerIds:   serverIds[:i+1]})
		require.NoError(t, err)
	}
	if rootN > 1 {
		rootPb, err = topoMgr.FetchTopology(
			ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
		require.NoError(t, err)
	}
	return s, rootPb, rootCli
}
