package stream_blast

import (
	"context"
	"strconv"
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

var _ = zlog.Info

func TestStreamBlast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wDir := storage.TestTmpDir()
	rootURI := topomgr.Prefix + "itest"

	rootTopology := itest.BootstrapDeployment(t, ctx, rootURI, wDir, itest.RootDefaultPort)
	s := make([]*servicelib.Service, 3)
	s[0] = itest.RunWalle(t, ctx, rootURI, "", rootTopology, wDir, itest.RootDefaultPort)
	defer s[0].Stop(t)
	for i := 1; i <= 2; i++ {
		s[i] = itest.RunWalle(
			t, ctx, rootURI, "", rootTopology, storage.TestTmpDir(), itest.RootDefaultPort+i)
		defer s[i].Kill(t)
	}

	rootD, err := wallelib.NewRootDiscovery(ctx, rootTopology)
	require.NoError(t, err)
	cli := wallelib.NewClient(ctx, rootD)
	topoMgr := topomgr.NewClient(cli)

	blastURIPrefix := "/blast/"
	blastURIs := 4
	topology, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: rootURI})
	require.NoError(t, err)
	serverIds := itest.ServerIdsSlice(topology.Servers)
	for i := 0; i < blastURIs; i++ {
		_, err = topoMgr.UpdateServerIds(ctx, &topomgr_pb.UpdateServerIdsRequest{
			TopologyUri: rootURI,
			StreamUri:   blastURIPrefix + strconv.Itoa(i),
			ServerIds:   serverIds,
		})
	}
	require.NoError(t, err)

	w := make([]*wallelib.Writer, blastURIs)
	for i := 0; i < blastURIs; i++ {
		var err error
		// use higher lease since this test is very CPU intensive.
		w[i], err = wallelib.WaitAndClaim(
			ctx, cli, blastURIPrefix+strconv.Itoa(i), "blastwriter:1001", 4*time.Second)
		require.NoError(t, err)
		defer w[i].Close()
	}
	// Test with full quorum.
	itest.PutBatch(t, 2000, 10, w[0])
	itest.PutBatch(t, 2000, 100, w[0])
	itest.PutBatch(t, 2000, 1000, w[0])
	itest.PutBatch(t, 2000, 1000, w...)

	// Test with one node down.
	defer servicelib.IptablesClearAll(t)
	servicelib.IptablesBlockPort(t, itest.RootDefaultPort+2)
	s[2].Kill(t)
	itest.PutBatch(t, 2000, 10, w[0])
	itest.PutBatch(t, 2000, 100, w[0])
	itest.PutBatch(t, 2000, 1000, w[0])
	itest.PutBatch(t, 2000, 1000, w...)
}
