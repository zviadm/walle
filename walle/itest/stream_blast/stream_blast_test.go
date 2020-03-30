package stream_blast

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

var _ = zlog.Info

func TestStreamBlast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wDir := walle.TestTmpDir()
	rootURI := "/topology/itest"
	blastURI := "/t1/blast"

	rootTopology := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.WalleDefaultPort)
	s := itest.RunWalle(t, ctx, rootURI, "", rootTopology, wDir, itest.WalleDefaultPort)
	defer s.Stop(t)
	for i := 1; i <= 3; i++ {
		s := itest.RunWalle(
			t, ctx, rootURI, "", rootTopology, walle.TestTmpDir(), itest.WalleDefaultPort+i)
		defer s.Kill(t)
	}

	rootD, err := wallelib.NewRootDiscovery(ctx, rootURI, rootTopology)
	require.NoError(t, err)
	cli := wallelib.NewClient(ctx, rootD)
	topoMgr := topomgr.NewClient(cli)

	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	var serverIds []string
	for serverId := range topology.Servers {
		serverIds = append(serverIds, serverId)
	}
	_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
		TopologyUri: rootURI,
		StreamUri:   blastURI,
		ServerIds:   serverIds,
	})
	require.NoError(t, err)

	w, _, err := wallelib.WaitAndClaim(
		ctx, cli, blastURI, "blastwriter:1001", time.Second)
	require.NoError(t, err)
	defer w.Close()

	t0 := time.Now()
	nBatch := 10000
	puts := make([]*wallelib.PutCtx, 0, nBatch)
	putIdx := 0
	for i := 0; i < nBatch; i++ {
		putCtx := w.PutEntry([]byte("testingoooo"))
		puts = append(puts, putCtx)

		if i-putIdx > 1000 {
			<-puts[putIdx].Done()
			require.NoError(t, puts[putIdx].Err())
			if puts[putIdx].Entry.EntryId%1000 == 0 {
				zlog.Info("TEST: putEntry success ", putCtx.Entry.EntryId)
			}
			putIdx += 1
		}
	}
	for i := putIdx; i < len(puts); i++ {
		<-puts[i].Done()
		require.NoError(t, puts[i].Err())
	}
	zlog.Info("TEST: processed all entries: ", len(puts), " ", time.Now().Sub(t0))
}
