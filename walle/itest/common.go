// package `itest` contains large end-to-end tests for WALLE system.
//
// End-to-end tests are expected to be large and complex, thus each
// test is placed in a separate package, making it easier to run them in
// parallel and manage their results separately.
package itest

import (
	"context"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/tt/servicelib"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

var _ = zlog.Info // Force import of `zlog`.

const (
	WallePkg         = "../../walle"
	WalleDefaultPort = 5005
)

func BootstrapDeployment(
	t *testing.T,
	ctx context.Context,
	rootURI string,
	storageDir string,
	port int) *walleapi.Topology {
	// Bootstrap WALLE `itest` deployment.
	sBootstrap, err := servicelib.RunGoService(
		ctx, WallePkg, []string{
			"-walle.root_uri", rootURI,
			"-walle.storage_dir", storageDir,
			"-walle.port", strconv.Itoa(port),
			"-walle.bootstrap_only",
		},
		"")
	require.NoError(t, err)
	sBootstrap.Wait(t)

	rootTopology, err := wallelib.TopologyFromFile(path.Join(storageDir, "root.pb"))
	require.NoError(t, err)
	return rootTopology
}

func RunWalle(
	t *testing.T,
	ctx context.Context,
	rootURI string,
	topologyURI string,
	rootTopology *walleapi.Topology,
	storageDir string,
	port int) *servicelib.Service {
	err := wallelib.TopologyToFile(rootTopology, path.Join(storageDir, "root.pb"))
	require.NoError(t, err)
	s, err := servicelib.RunGoService(
		ctx, WallePkg, []string{
			"-walle.root_uri", rootURI,
			"-walle.topology_uri", topologyURI,
			"-walle.storage_dir", storageDir,
			"-walle.port", strconv.Itoa(port),
		},
		strconv.Itoa(port))
	require.NoError(t, err)
	return s
}
