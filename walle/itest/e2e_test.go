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

const (
	wallePkg = "github.com/zviadm/walle/walle"
)

func TestE2ESimple(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w1Dir := walle.StorageTmpTestDir()
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

	s, err = servicelib.RunGoService(
		ctx, wallePkg, []string{
			"-walle.storage_dir", w1Dir,
			"-walle.root_uri", "/topology/itest",
			"-walle.port", "5005",
			"-logtostderr",
		},
		"5005")
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	defer s.Stop(t)
}
