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
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

func TestCrashingQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rootURI := topomgr.Prefix + "itest"
	wDir := storage.TestTmpDir()

	rootTopology := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.WalleDefaultPort)
	s := make([]*servicelib.Service, 3)
	s[0] = itest.RunWalle(t, ctx, rootURI, "", rootTopology, wDir, itest.WalleDefaultPort)
	defer s[0].Kill(t)

	rootD, err := wallelib.NewRootDiscovery(ctx, rootTopology)
	require.NoError(t, err)
	cli := wallelib.NewClient(ctx, rootD)
	topoMgr := topomgr.NewClient(cli)

	var serverIds []string
	for i := 1; i <= 2; i++ {
		s[i] = itest.RunWalle(t, ctx, rootURI, "", rootTopology, storage.TestTmpDir(), itest.WalleDefaultPort+i)
		defer s[i].Kill(t)
		topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
		require.NoError(t, err)
		serverIds = itest.ServerIdsSlice(topology.Servers)
		_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
			TopologyUri: rootURI,
			StreamUri:   rootURI,
			ServerIds:   serverIds,
		})
		require.NoError(t, err)
	}
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   "/t1/blast",
		ServerIds:   serverIds,
	})
	require.NoError(t, err)

	defer servicelib.IptablesClearAll(t)
	crashWG := sync.WaitGroup{}
	crashWG.Add(1)
	crashC := make(chan time.Duration)
	go crashLoop(t, s, crashC, &crashWG)
	defer func() {
		close(crashC)
		crashWG.Wait()
	}()

	w, e, err := wallelib.WaitAndClaim(
		ctx, cli, "/t1/blast", "blastwriter:1001", time.Second)
	require.NoError(t, err)
	defer w.Close()
	require.EqualValues(t, 0, e.EntryId)
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
		servicelib.IptablesBlockPort(t, itest.WalleDefaultPort+idx)
		zlog.Infof("TEST: killing s[%d] process", idx)
		s[idx].Kill(t)

		_, ok = <-crashC
		if !ok {
			return
		}
		servicelib.IptablesUnblockPort(t, itest.WalleDefaultPort+idx)
		zlog.Infof("TEST: starting s[%d] process", idx)
		s[idx].Start(t, ctx)
		crashC <- 0
	}
}
