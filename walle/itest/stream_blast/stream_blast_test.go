package stream_blast

import (
	"context"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/tt/servicelib"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/zlog"
)

func TestStreamBlast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer servicelib.KillAll()

	_, rootPb, rootCli := itest.SetupRootNodes(ctx, t, 1)
	clusterURI := path.Join(topomgr.Prefix, "bench")
	s, serverIds := itest.SetupClusterNodes(ctx, t, rootPb, rootCli, clusterURI, 3)

	benchURIPrefix := "/bench"
	benchURIs := 4
	for i := 0; i < benchURIs; i++ {
		ww, err := servicelib.RunGoService(
			ctx, "../../wctl", append(
				[]string{"-c", clusterURI, "crupdate", path.Join(benchURIPrefix, strconv.Itoa(i))}, serverIds...),
			"")
		require.NoError(t, err)
		require.EqualValues(t, 0, ww.Wait())
	}

	// Test with full quorum.
	zlog.Info("TEST: ---------- FULL QUORUM")
	benchAll(ctx, t, clusterURI, benchURIPrefix)

	// Test with one node down.
	zlog.Info("TEST: ---------- NODES 2 / 3")
	defer require.NoError(t, servicelib.IptablesClearAll())
	require.NoError(t, servicelib.IptablesBlockPort(itest.RootDefaultPort+2))
	s[2].Kill()
	benchAll(ctx, t, clusterURI, benchURIPrefix)
}

func benchAll(ctx context.Context, t *testing.T, clusterURI string, benchURIPrefix string) {
	ww, err := servicelib.RunGoService(
		ctx, "../../wctl", []string{
			"-c", clusterURI, "bench", "-prefix", benchURIPrefix, "-lease=500ms",
			"-streams", "1", "-qps", "500", "-kbs", "500", "-time", "2s"},
		"")
	require.NoError(t, err)
	require.EqualValues(t, 0, ww.Wait())
	ww, err = servicelib.RunGoService(
		ctx, "../../wctl", []string{
			"-c", clusterURI, "bench", "-prefix", benchURIPrefix, "-lease=500ms",
			"-streams", "2", "-qps", "500", "-kbs", "500", "-time", "2s"},
		"")
	require.NoError(t, err)
	require.EqualValues(t, 0, ww.Wait())
	ww, err = servicelib.RunGoService(
		ctx, "../../wctl", []string{
			"-c", clusterURI, "bench", "-prefix", benchURIPrefix, "-lease=500ms",
			"-streams", "4", "-qps", "500", "-kbs", "500", "-time", "2s"},
		"")
	require.NoError(t, err)
	require.EqualValues(t, 0, ww.Wait())
}
