package itest

import (
	"context"
	"testing"
	"time"

	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/tt/servicelib"
	"github.com/zviadm/walle/walle"
)

var _ = glog.Info

func TestRootTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w1Dir := walle.StorageTmpTestDir()
	s, err := servicelib.RunGoService(
		ctx, "../walle/walle",
		"-walle.db_path", w1Dir,
		"-walle.root_uri", "/topology/itest",
		"-walle.topology_uri", "/topology/itest",
		"-walle.port", "5005",
		"-walle.root_seeds", "s1:localhost:5005",
		"-logtostderr")
	require.NoError(t, err)
	defer s.Stop(t)
}

func runWalle(ctx context.Context) {

}
