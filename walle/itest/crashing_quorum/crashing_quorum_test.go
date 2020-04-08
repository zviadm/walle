package crashing_quorum

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/tt/servicelib"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

func TestCrashingQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer servicelib.KillAll()
	defer require.NoError(t, servicelib.IptablesClearAll())

	s, rootPb, rootCli := itest.SetupRootNodes(t, ctx, 3)

	streamURI := "/t1/crashing"
	itest.CreateStream(
		t, ctx, rootCli, rootPb.RootUri, streamURI,
		rootPb.Streams[rootPb.RootUri].ServerIds)

	crashC := make(chan time.Duration)
	crashErr := make(chan error, 1)
	go crashLoop(ctx, s, crashC, crashErr)
	defer func() {
		close(crashC)
		err, _ := <-crashErr
		require.NoError(t, err)
	}()

	w, err := wallelib.WaitAndClaim(
		ctx, rootCli, streamURI, "blastwriter:1001", time.Second)
	require.NoError(t, err)
	defer w.Close()
	require.EqualValues(t, 0, w.Committed().EntryId)
	zlog.Info("TEST: writer claimed for ", streamURI)

	for i := 0; i < 4; i++ {
		zlog.Info("TEST: CRASH ITERATION --- ", i)
		select {
		case crashC <- 100 * time.Millisecond:
		case err := <-crashErr:
			require.NoError(t, err)
		}
		itest.PutBatch(t, 1000, 100, w)
		select {
		case crashC <- 0:
		case err := <-crashErr:
			require.NoError(t, err)
		}
		select {
		case <-crashC:
		case err := <-crashErr:
			require.NoError(t, err)
		}
	}
}

func crashLoop(
	ctx context.Context,
	s []*servicelib.Service,
	crashC chan time.Duration,
	crashErr chan error) (err error) {
	defer func() {
		crashErr <- err
		close(crashErr)
	}()
	for i := 0; ; i++ {
		delay, ok := <-crashC
		if !ok {
			return nil
		}
		idx := i % len(s)
		time.Sleep(delay)
		if err := servicelib.IptablesBlockPort(
			itest.RootDefaultPort + idx); err != nil {
			return err
		}
		zlog.Infof("TEST: killing s[%d] process", idx)
		s[idx].Kill()

		_, ok = <-crashC
		if !ok {
			return nil
		}
		servicelib.IptablesClearAll()
		// servicelib.IptablesUnblockPort(t, itest.RootDefaultPort+idx)
		zlog.Infof("TEST: starting s[%d] process", idx)
		s[idx].Start(ctx)
		crashC <- 0
	}
}
