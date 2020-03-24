package itest

import (
	"context"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cliDir := walle.TestTmpDir()
	cliRootPb := path.Join(cliDir, "root.pb")
	// cliTopoPb := path.Join(cliDir, "topology.pb")
	w1Dir := walle.TestTmpDir()

	// Bootstrap WALLE `itest` deployment.
	s, err := servicelib.RunGoService(
		ctx, wallePkg, []string{
			"-walle.storage_dir", w1Dir,
			"-walle.root_uri", "/topology/itest",
			"-walle.port", "5005",
			"-walle.bootstrap_only",
			"-logtostderr",
		},
		"")
	require.NoError(t, err)
	s.Wait(t)

	// Copy `root.pb` so client can use it for discovery.
	rootPbData, err := ioutil.ReadFile(path.Join(w1Dir, "root.pb"))
	require.NoError(t, err)
	err = ioutil.WriteFile(cliRootPb, rootPbData, 0644)
	require.NoError(t, err)

	// Start WALLE server.
	s, err = servicelib.RunGoService(
		ctx, wallePkg, []string{
			"-walle.storage_dir", w1Dir,
			"-walle.root_uri", "/topology/itest",
			"-walle.port", "5005",
			"-logtostderr",
		},
		"5005")
	require.NoError(t, err)
	defer s.Stop(t)

	// Read and update root topology to add a new cluster.
	cli, err := wallelib.NewClientForTopology(ctx, "/topology/itest", cliRootPb, "", "")
	require.NoError(t, err)
	w, entry, err := wallelib.ClaimWriter(ctx, cli, "/topology/itest", "e2e_test:1001", time.Second)
	require.NoError(t, err)
	topo, err := wallelib.TopologyFromEntry(entry)
	require.NoError(t, err)
	require.EqualValues(t, 1, topo.Version)
	topo.Version += 1
	topo.Streams["/cluster_a/1"] = &walleapi.StreamTopology{
		Version:   topo.Version,
		ServerIds: topo.Streams["/topology/itest"].ServerIds,
	}
	entryData, err := topo.Marshal()
	require.NoError(t, err)
	_, waitC := w.PutEntry(entryData)
	err = <-waitC
	require.NoError(t, err)
	w.Close()

	// Wait a bit to make sure new topology is propagated to the server.
	time.Sleep(5 * time.Second)

	w, entry, err = wallelib.ClaimWriter(ctx, cli, "/cluster_a/1", "e2e_test:1001", time.Second)
	require.NoError(t, err)
	require.EqualValues(t, 0, entry.EntryId)
	w.Close()
}
