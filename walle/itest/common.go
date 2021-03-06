// Package itest contains large end-to-end tests for WALLE system.
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
)

const (
	// WallePkg points to `walle` binary, relative to tests in `itest` package.
	WallePkg = "../../walle"
	// RootDefaultPort is starting port for root cluster nodes.
	RootDefaultPort = 5005
	// ClusterDefaultPort is starting port for regular nodes.
	ClusterDefaultPort = 5055
)

// BootstrapDeployment bootstraps new WALLE deployment.
func BootstrapDeployment(
	ctx context.Context,
	t *testing.T,
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

// RunWalle starts new WALLE server process.
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
			"-stats.instance_name", "walle" + strconv.Itoa(port),
			"-debug.addr", "127.0.0.1:" + strconv.Itoa(port+1000),
			"-walle.target_mem_mb=100",
		},
		strconv.Itoa(port))
}
