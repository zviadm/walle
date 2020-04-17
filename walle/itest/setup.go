package itest

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/tt/servicelib"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/walle/wallelib/topolib"
)

// SetupRootNodes bootstraps new WALLE deployment and sets up root servers to serve it.
func SetupRootNodes(
	ctx context.Context, t *testing.T, rootN int) (
	s []*servicelib.Service, rootPb *walleapi.Topology, rootCli wallelib.Client) {
	rootURI := topomgr.Prefix + "itest"
	wDir0 := storage.TestTmpDir()
	rootPb = BootstrapDeployment(ctx, t, rootURI, wDir0, RootDefaultPort)
	mx := sync.Mutex{}
	var runErr error
	s = make([]*servicelib.Service, rootN)
	wg := sync.WaitGroup{}
	wg.Add(len(s))
	for idx := range s {
		go func(idx int) {
			defer wg.Done()
			wDir := wDir0
			if idx > 0 {
				wDir = storage.TestTmpDir()
			}
			ss, err := RunWalle(ctx, rootPb, rootURI, wDir, RootDefaultPort+idx)
			mx.Lock()
			defer mx.Unlock()
			if err != nil {
				runErr = err
				return
			}
			s[idx] = ss
		}(idx)
	}
	wg.Wait()
	require.NoError(t, runErr)

	var err error
	rootCli, err = wallelib.NewClientFromRootPb(ctx, rootPb, "")
	require.NoError(t, err)
	topoMgr := topolib.NewClient(rootCli)
	topology, err := topoMgr.FetchTopology(
		ctx, &topomgr_pb.FetchTopologyRequest{ClusterUri: rootURI})
	require.NoError(t, err)
	serverIds := serverIdsSlice(topology.Servers)
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
		_, err = topoMgr.CrUpdateStream(ctx, &topomgr_pb.CrUpdateStreamRequest{
			ClusterUri: rootURI,
			StreamUri:  rootURI,
			ServerIds:  serverIds[:i+1]})
		require.NoError(t, err)
	}
	if rootN > 1 {
		rootPb, err = topoMgr.FetchTopology(
			ctx, &topomgr_pb.FetchTopologyRequest{ClusterUri: rootURI})
		require.NoError(t, err)
	}
	return s, rootPb, rootCli
}

// SetupClusterNodes creates new cluster and sets up WALLE servers to serve it.
func SetupClusterNodes(
	ctx context.Context,
	t *testing.T,
	rootPb *walleapi.Topology,
	rootCli wallelib.Client,
	clusterURI string,
	clusterN int) (s []*servicelib.Service, serverIds []string) {

	CreateStream(
		ctx, t, rootCli, rootPb.RootUri, clusterURI, rootPb.Streams[rootPb.RootUri].ServerIds)
	mx := sync.Mutex{}
	var runErr error
	s = make([]*servicelib.Service, clusterN)
	wg := sync.WaitGroup{}
	wg.Add(len(s))
	for idx := range s {
		go func(idx int) {
			defer wg.Done()
			ss, err := RunWalle(ctx, rootPb, clusterURI, storage.TestTmpDir(), ClusterDefaultPort+idx)
			mx.Lock()
			defer mx.Unlock()
			if err != nil {
				runErr = err
				return
			}
			s[idx] = ss
		}(idx)
	}
	wg.Wait()
	require.NoError(t, runErr)
	topoMgr := topolib.NewClient(rootCli)
	clusterPb, err := topoMgr.FetchTopology(
		ctx, &topomgr_pb.FetchTopologyRequest{ClusterUri: clusterURI})
	require.NoError(t, err)
	return s, serverIdsSlice(clusterPb.Servers)
}

// CreateStream creates new stream in WALLE cluster.
func CreateStream(
	ctx context.Context,
	t *testing.T,
	root wallelib.Client,
	clusterURI string,
	streamURI string,
	serverIds []string) {
	topoMgr := topolib.NewClient(root)
	_, err := topoMgr.CrUpdateStream(ctx, &topomgr_pb.CrUpdateStreamRequest{
		ClusterUri: clusterURI,
		StreamUri:  streamURI,
		ServerIds:  serverIds})
	require.NoError(t, err)
}

func serverIdsSlice(servers map[string]*walleapi.ServerInfo) []string {
	var serverIds []string
	for serverId := range servers {
		serverIds = append(serverIds, serverId)
	}
	return serverIds
}
