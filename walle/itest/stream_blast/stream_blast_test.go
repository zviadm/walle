package stream_blast

import (
	"context"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/zviadm/tt/servicelib"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/zlog"
)

func TestStreamBlast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer servicelib.KillAll(t)

	_, rootPb, rootCli := itest.SetupRootNodes(t, ctx, 1)
	clusterURI := path.Join(topomgr.Prefix, "bench")
	s, serverIds := itest.SetupClusterNodes(t, ctx, rootPb, rootCli, clusterURI, 3)

	benchURIPrefix := "/bench"
	benchURIs := 4
	for i := 0; i < benchURIs; i++ {
		servicelib.RunGoService(
			t, ctx, "../../wctl", append(
				[]string{"-c", clusterURI, "create", path.Join(benchURIPrefix, strconv.Itoa(i))}, serverIds...),
			"").Wait(t)
	}

	// Test with full quorum.
	zlog.Info("TEST: ---------- FULL QUORUM")
	benchAll(t, ctx, clusterURI, benchURIPrefix)

	// Test with one node down.
	zlog.Info("TEST: ---------- NODES 2 / 3")
	defer servicelib.IptablesClearAll(t)
	servicelib.IptablesBlockPort(t, itest.RootDefaultPort+2)
	s[2].Kill(t)
	benchAll(t, ctx, clusterURI, benchURIPrefix)
}

func benchAll(t *testing.T, ctx context.Context, clusterURI string, benchURIPrefix string) {
	servicelib.RunGoService(
		t, ctx, "../../wctl", []string{
			"-c", clusterURI, "bench", "-prefix", benchURIPrefix,
			"-streams", "1", "-qps", "100", "-kbs", "100", "-time", "2s"},
		"").Wait(t)
	servicelib.RunGoService(
		t, ctx, "../../wctl", []string{
			"-c", clusterURI, "bench", "-prefix", benchURIPrefix,
			"-streams", "2", "-qps", "100", "-kbs", "100", "-time", "2s"},
		"").Wait(t)
	servicelib.RunGoService(
		t, ctx, "../../wctl", []string{
			"-c", clusterURI, "bench", "-prefix", benchURIPrefix,
			"-streams", "4", "-qps", "100", "-kbs", "100", "-time", "2s"},
		"").Wait(t)
}
