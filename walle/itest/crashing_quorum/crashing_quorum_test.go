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
	defer s[0].Stop(t)
	s[1] = itest.RunWalle(t, ctx, rootURI, "", rootTopology, walle.TestTmpDir(), itest.WalleDefaultPort+1)
	defer s[1].Stop(t)
	s[2] = itest.RunWalle(t, ctx, rootURI, "", rootTopology, walle.TestTmpDir(), itest.WalleDefaultPort+2)
	defer s[2].Stop(t)

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

	claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	w, e, err := wallelib.WaitAndClaim(claimCtx, cli, "/t1/blast", "", wallelib.LeaseMinimum)
	require.NoError(t, err)
	defer w.Close(true)
	require.EqualValues(t, 0, e.EntryId)

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

	// blastCtx, blastCancel := context.WithCancel(ctx)
	// blastDone := make(chan struct{})
	// go func() {
	// 	defer close(blastDone)
	// 	defer w.Close(true)
	// 	for blastCtx.Err() == nil {
	// 		e, errC := w.PutEntry([]byte("testingoooo"))
	// 		zlog.Info("putting entry ", e.EntryId)
	// 		select {
	// 		case err := <-errC:
	// 			require.NoError(t, err)
	// 		case <-time.After(500 * time.Millisecond):
	// 			require.FailNow(t, "putEntry timedout, exiting!")
	// 		}
	// 	}
	// }()
	// defer blastCancel()

	// for i := 0; i < 5; i++ {
	// 	// servicelib.IptablesBlockPort(t, itest.WalleDefaultPort+i%3)
	// 	time.Sleep(2 * time.Second)
	// 	// servicelib.IptablesUnblockPort(t, itest.WalleDefaultPort+i%3)
	// }
	// blastCancel()
	// <-blastDone
}