package itest

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/tt/servicelib"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/wallelib"
)

var _ = glog.Info

const (
	wallePkg = "../walle"
)

func TestE2ESimple(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w1Dir := walle.TestTmpDir()
	rootURI := "/topology/itest"

	// Bootstrap WALLE `itest` deployment.
	sBootstrap, err := servicelib.RunGoService(
		ctx, wallePkg, []string{
			"-walle.storage_dir", w1Dir,
			"-walle.root_uri", rootURI,
			"-walle.port", "5005",
			"-walle.bootstrap_only",
			"-logtostderr",
		},
		"")
	require.NoError(t, err)
	sBootstrap.Wait(t)

	rootTopology, err := wallelib.TopologyFromFile(path.Join(w1Dir, "root.pb"))
	require.NoError(t, err)
	rootD, err := wallelib.NewRootDiscovery(ctx, rootURI, rootTopology)
	require.NoError(t, err)
	cli := wallelib.NewClient(ctx, rootD)
	topoMgr := walle.NewTopoMgrClient(cli)

	// Start initial WALLE server.
	s, err := servicelib.RunGoService(
		ctx, wallePkg, []string{
			"-walle.storage_dir", w1Dir,
			"-walle.root_uri", rootURI,
			"-walle.port", "5005",
			"-logtostderr",
		},
		"5005")
	require.NoError(t, err)
	defer s.Stop(t)

	services := []*servicelib.Service{s}
	for i := 0; i < 4; i++ {
		s := expandTopology(t, ctx, walle.TestTmpDir(), strconv.Itoa(5006+i), topoMgr, rootURI)
		defer s.Stop(t)
		services = append(services, s)
	}
	for _, s := range services {
		shrinkTopology(t, ctx, s, topoMgr, rootURI)
	}
}

func expandTopology(
	t *testing.T,
	ctx context.Context,
	wDir string,
	port string,
	topoMgr topomgr.TopoManagerClient,
	rootURI string) *servicelib.Service {

	topology, err := topoMgr.FetchTopology(ctx, &topomgr.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	err = wallelib.TopologyToFile(topology, path.Join(wDir, "root.pb"))
	require.NoError(t, err)

	// Start WALLE server.
	s, err := servicelib.RunGoService(
		ctx, wallePkg, []string{
			"-walle.storage_dir", wDir,
			"-walle.root_uri", rootURI,
			"-walle.port", port,
			"-logtostderr",
		},
		port)
	require.NoError(t, err)

	topology, err = topoMgr.FetchTopology(ctx, &topomgr.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	topology.Version += 1
	serverId := serverIdsDiff(t, topology.Servers, topology.Streams[rootURI].ServerIds)
	topology.Streams[rootURI].ServerIds = append(topology.Streams[rootURI].ServerIds, serverId)
	_, err = topoMgr.UpdateTopology(ctx, &topomgr.UpdateTopologyRequest{
		TopologyUri: rootURI,
		Topology:    topology,
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
	topoMgr topomgr.TopoManagerClient,
	rootURI string) {
	topology, err := topoMgr.FetchTopology(ctx, &topomgr.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	topology.Version += 1
	topology.Streams[rootURI].ServerIds = topology.Streams[rootURI].ServerIds[1:]
	_, err = topoMgr.UpdateTopology(ctx, &topomgr.UpdateTopologyRequest{
		TopologyUri: rootURI,
		Topology:    topology,
	})
	require.NoError(t, err)
	s.Stop(t)
}
