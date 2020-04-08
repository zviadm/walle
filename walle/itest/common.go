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

// Must be called in main test function.
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
	sBootstrap, err := servicelib.RunGoService(
		ctx, WallePkg, []string{
			"-walle.bootstrap_uri", rootURI,
			"-walle.storage_dir", storageDir,
			"-walle.port", strconv.Itoa(port),
		},
		"")
	require.NoError(t, err)
	eCode := sBootstrap.Wait()
	require.EqualValues(t, 0, eCode)

	rootPbFile := path.Join(storageDir, "root.pb")
	rootTopology, err := wallelib.TopologyFromFile(rootPbFile)
	require.NoError(t, err)
	os.Setenv(wallelib.EnvRootPb, rootPbFile)
	return rootTopology
}

func RunWalle(
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string,
	storageDir string,
	port int) (*servicelib.Service, error) {
	if err := wallelib.TopologyToFile(
		rootPb, path.Join(storageDir, "root.pb")); err != nil {
		return nil, err
	}
	return servicelib.RunGoService(
		ctx, WallePkg, []string{
			"-walle.cluster_uri", clusterURI,
			"-walle.storage_dir", storageDir,
			"-walle.port", strconv.Itoa(port),
			// use higher lease in integration testing since bunch of servers run
			// in a single docker container.
			"-walle.topomgr_lease", "1s",
		},
		strconv.Itoa(port))
}

func ServerIdsSlice(servers map[string]*walleapi.ServerInfo) []string {
	var serverIds []string
	for serverId := range servers {
		serverIds = append(serverIds, serverId)
	}
	return serverIds
}
