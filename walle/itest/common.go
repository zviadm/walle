// package `itest` contains large end-to-end tests for WALLE system.
//
// End-to-end tests are expected to be large and complex, thus each
// test is placed in a separate package, making it easier to run them in
// parallel and manage their results separately.
package itest

import (
	"context"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/tt/servicelib"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

var _ = zlog.Info // Force import of `zlog`.

const (
	WallePkg           = "../../walle"
	RootDefaultPort    = 5005
	ClusterDefaultPort = 5055
)

func BootstrapDeployment(
	t *testing.T,
	ctx context.Context,
	rootURI string,
	storageDir string,
	port int) *walleapi.Topology {
	if testing.Short() {
		t.SkipNow()
	}
	// Bootstrap WALLE `itest` deployment.
	sBootstrap := servicelib.RunGoService(
		t, ctx, WallePkg, []string{
			"-walle.bootstrap_uri", rootURI,
			"-walle.storage_dir", storageDir,
			"-walle.port", strconv.Itoa(port),
		},
		"")
	sBootstrap.Wait(t)

	rootPbFile := path.Join(storageDir, "root.pb")
	rootTopology, err := wallelib.TopologyFromFile(rootPbFile)
	require.NoError(t, err)
	os.Setenv(wallelib.EnvRootPb, rootPbFile)
	return rootTopology
}

func RunWalle(
	t *testing.T,
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string,
	storageDir string,
	port int) *servicelib.Service {
	err := wallelib.TopologyToFile(rootPb, path.Join(storageDir, "root.pb"))
	require.NoError(t, err)
	s := servicelib.RunGoService(
		t, ctx, WallePkg, []string{
			"-walle.cluster_uri", clusterURI,
			"-walle.storage_dir", storageDir,
			"-walle.port", strconv.Itoa(port),
			// use higher lease in integration testing since bunch of servers run
			// in a single docker container.
			"-walle.topomgr_lease", "1s",
		},
		strconv.Itoa(port))
	return s
}

func ServerIdsSlice(servers map[string]*walleapi.ServerInfo) []string {
	var serverIds []string
	for serverId := range servers {
		serverIds = append(serverIds, serverId)
	}
	return serverIds
}
