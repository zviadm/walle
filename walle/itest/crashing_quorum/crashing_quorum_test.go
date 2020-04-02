package crashing_quorum

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/tt/servicelib"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

func TestCrashingQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer servicelib.KillAll(t)
	defer servicelib.IptablesClearAll(t)

	s, rootPb := itest.SetupRootNodes(t, ctx, 3)

	cli, err := wallelib.NewClientFromRootPb(ctx, rootPb, "")
	require.NoError(t, err)
	topoMgr := topomgr.NewClient(cli)
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootPb.RootUri,
		StreamUri:   "/t1/blast",
		ServerIds:   itest.ServerIdsSlice(rootPb.Servers),
	})
	require.NoError(t, err)

	crashWG := sync.WaitGroup{}
	crashWG.Add(1)
	crashC := make(chan time.Duration)
	go crashLoop(t, s, crashC, &crashWG)
	defer func() {
		close(crashC)
		crashWG.Wait()
	}()

	w, err := wallelib.WaitAndClaim(
		ctx, cli, "/t1/blast", "blastwriter:1001", time.Second)
	require.NoError(t, err)
	defer w.Close()
	require.EqualValues(t, 0, w.Committed().EntryId)
	zlog.Info("TEST: writer claimed for /t1/blast")

	for i := 0; i < 4; i++ {
		zlog.Info("TEST: CRASH ITERATION --- ", i)
		crashC <- 100 * time.Millisecond
		itest.PutBatch(t, 1000, 100, w)
		crashC <- 0
		<-crashC
	}
}

func crashLoop(t *testing.T, s []*servicelib.Service, crashC chan time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	for i := 0; ; i++ {
		delay, ok := <-crashC
		if !ok {
			return
		}
		idx := i % len(s)
		time.Sleep(delay)
		servicelib.IptablesBlockPort(t, itest.RootDefaultPort+idx)
		zlog.Infof("TEST: killing s[%d] process", idx)
		s[idx].Kill(t)

		_, ok = <-crashC
		if !ok {
			return
		}
		servicelib.IptablesUnblockPort(t, itest.RootDefaultPort+idx)
		zlog.Infof("TEST: starting s[%d] process", idx)
		s[idx].Start(t, ctx)
		crashC <- 0
	}
}
