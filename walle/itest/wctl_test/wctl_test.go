package wctl_test

import (
	"context"
	"testing"
	"time"

	"github.com/zviadm/tt/servicelib"
	"github.com/zviadm/walle/walle/itest"
)

func TestWCTL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer servicelib.KillAll(t)

	_, rootPb, _ := itest.SetupRootNodes(t, ctx, 1)

	s := servicelib.RunGoService(t, ctx, "../../wctl", []string{"servers"}, "")
	s.Wait(t)
	_ = rootPb
}
