package stream_blast

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/tt/servicelib"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
)

func TestStreamBlast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer servicelib.KillAll(t)

	_, rootPb, rootCli := itest.SetupRootNodes(t, ctx, 1)
	clusterURI := topomgr.Prefix + "blast"
	s, serverIds := itest.SetupClusterNodes(t, ctx, rootPb, rootCli, clusterURI, 3)

	blastURIPrefix := "/blast/"
	blastURIs := 4
	for i := 0; i < blastURIs; i++ {
		itest.CreateStream(t, ctx, rootCli, clusterURI, blastURIPrefix+strconv.Itoa(i), serverIds)
	}
	cli, err := wallelib.NewClientFromRootPb(ctx, rootPb, clusterURI)
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
