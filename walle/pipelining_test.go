package walle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

func TestPutEntryPipelining(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, topoSimple, TestTmpDir())

	w, _, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w.Close(false)

	nBatch := 50
	t0 := time.Now()
	for i := 0; i < nBatch; i++ {
		_, errC := w.PutEntry([]byte("testingoooo"))
		select {
		case err := <-errC:
			require.NoError(t, err)
		case <-time.After(500 * time.Millisecond):
			require.FailNow(t, "putEntry timedout, exiting!")
		}
	}
	zlog.Infof("putting entries: %d, delta: %s", nBatch, time.Now().Sub(t0))

	t0 = time.Now()
	var errCs []<-chan error
	for i := 0; i < nBatch; i++ {
		_, errC := w.PutEntry([]byte("testingoooo"))
		errCs = append(errCs, errC)
	}
	for _, errC := range errCs {
		select {
		case err := <-errC:
			require.NoError(t, err)
		case <-time.After(500 * time.Millisecond):
			require.FailNow(t, "putEntry timedout, exiting!")
		}
	}
	zlog.Infof("putting entries: %d, delta: %s", nBatch, time.Now().Sub(t0))
}
