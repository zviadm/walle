package crashing_quorum

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/tt/servicelib"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

func TestCrashingQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rootURI := "/topology/itest"
	wDir := walle.TestTmpDir()

	rootTopology := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.WalleDefaultPort)
	s := make([]*servicelib.Service, 3)
	s[0] = itest.RunWalle(t, ctx, rootURI, "", rootTopology, wDir, itest.WalleDefaultPort)
	defer s[0].Kill(t)
	s[1] = itest.RunWalle(t, ctx, rootURI, "", rootTopology, walle.TestTmpDir(), itest.WalleDefaultPort+1)
	defer s[1].Kill(t)
	s[2] = itest.RunWalle(t, ctx, rootURI, "", rootTopology, walle.TestTmpDir(), itest.WalleDefaultPort+2)
	defer s[2].Kill(t)

	rootD, err := wallelib.NewRootDiscovery(ctx, rootURI, rootTopology)
	require.NoError(t, err)
	cli := wallelib.NewClient(ctx, rootD)
	topoMgr := topomgr.NewClient(cli)

	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	var serverIds = []string{rootTopology.Streams[rootURI].ServerIds[0]}
	for serverId := range topology.Servers {
		if serverId == serverIds[0] {
			continue
		}
		serverIds = append(serverIds, serverId)
	}
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   rootURI,
		ServerIds:   serverIds[:2],
	})
	require.NoError(t, err)
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   rootURI,
		ServerIds:   serverIds[:3],
	})
	require.NoError(t, err)
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   "/t1/blast",
		ServerIds:   serverIds,
	})
	require.NoError(t, err)

	defer servicelib.IptablesClearAll(t)
	servicelib.IptablesBlockPort(t, itest.WalleDefaultPort+1)
	s[1].Kill(t)
	zlog.Info("TEST: killed s[1] process")

	claimCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	w, e, err := wallelib.WaitAndClaim(claimCtx, cli, "/t1/blast", "blastwriter:1001", time.Second)
	require.NoError(t, err)
	defer w.Close()
	require.EqualValues(t, 0, e.EntryId)
	zlog.Info("TEST: writer claimed for /t1/blast")

	nBatch := 200
	errCs := make([]<-chan error, nBatch)
	for i := 0; i < nBatch; i++ {
		_, errC := w.PutEntry([]byte("testingoooo"))
		errCs[i] = errC
	}

	time.Sleep(2 * time.Second)
	servicelib.IptablesUnblockPort(t, itest.WalleDefaultPort+1)
	s[1].Start(t, ctx)
	zlog.Info("TEST: s[1] started")

	time.Sleep(5 * time.Second)
	wState, _ := w.WriterState()
	require.Equal(t, wallelib.Exclusive, wState)

	servicelib.IptablesBlockPort(t, itest.WalleDefaultPort+2)
	s[2].Kill(t)
	zlog.Info("TEST: killed s[2] process")

	for _, errC := range errCs {
		select {
		case err := <-errC:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "putEntry timedout, exiting!")
		}
	}
	zlog.Info("TEST: processed all entries: ", nBatch)
}
