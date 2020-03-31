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
	rootURI := "/topology/itest"
	wDir := storage.TestTmpDir()

	rootTopology := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.WalleDefaultPort)
	s := make([]*servicelib.Service, 3)
	s[0] = itest.RunWalle(t, ctx, rootURI, "", rootTopology, wDir, itest.WalleDefaultPort)
	defer s[0].Kill(t)

	rootD, err := wallelib.NewRootDiscovery(ctx, rootURI, rootTopology)
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
	crashCtx, crashCancel := context.WithCancel(ctx)
	crashWG := sync.WaitGroup{}
	crashWG.Add(1)
	go crashLoop(t, crashCtx, s, &crashWG)
	defer func() {
		crashCancel()
		crashWG.Wait()
	}()

	w, e, err := wallelib.WaitAndClaim(
		ctx, cli, "/t1/blast", "blastwriter:1001", time.Second)
	require.NoError(t, err)
	defer w.Close()
	require.EqualValues(t, 0, e.EntryId)
	zlog.Info("TEST: writer claimed for /t1/blast")

	itest.PutBatch(t, w, 20000, 100)
}

func crashLoop(t *testing.T, ctx context.Context, s []*servicelib.Service, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; ; i++ {
		idx := i % len(s)
		servicelib.IptablesBlockPort(t, itest.WalleDefaultPort+idx)
		zlog.Infof("TEST: killing s[%d] process", idx)
		s[idx].Kill(t)

		select {
		case <-ctx.Done():
			return
		case <-time.After(8 * time.Second):
		}

		servicelib.IptablesUnblockPort(t, itest.WalleDefaultPort+idx)
		zlog.Infof("TEST: starting s[%d] process", idx)
		s[idx].Start(t, context.Background())
	}
}
