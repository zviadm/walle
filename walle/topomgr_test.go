package walle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var rootPb3Node = &walleapi.Topology{
	RootUri: "/cluster/root",
	Streams: map[string]*walleapi.StreamTopology{
		"/cluster/root": &walleapi.StreamTopology{
			Version:   3,
			ServerIds: []string{"\x00\x01", "\x00\x02", "\x00\x03"},
		},
		"/cluster/t1": &walleapi.StreamTopology{
			Version:   3,
			ServerIds: []string{"\x00\x01", "\x00\x02", "\x00\x03"},
		},
	},
	Servers: map[string]*walleapi.ServerInfo{
		"\x00\x01": &walleapi.ServerInfo{Address: "localhost1:1001"},
		"\x00\x02": &walleapi.ServerInfo{Address: "localhost2:1001"},
		"\x00\x03": &walleapi.ServerInfo{Address: "localhost3:1001"},
	},
}

func TestTopoMgrApi(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, rootPb3Node, storage.TestTmpDir())

	clusterURI := "/cluster/t1"
	topoMgr := topomgr.NewManager(c, "topomgr_test:1001")
	defer topoMgr.Close()
	_, err := topoMgr.FetchTopology(ctx,
		&topomgr_pb.FetchTopologyRequest{ClusterUri: clusterURI})
	require.Error(t, err)
	require.EqualValues(t, codes.Unavailable, status.Convert(err).Code())

	topoMgr.Manage(clusterURI)
	time.Sleep(time.Second) // Wait for topomgr to become master.
	_, err = topoMgr.FetchTopology(ctx,
		&topomgr_pb.FetchTopologyRequest{ClusterUri: clusterURI})
	require.NoError(t, err)

	// TODO(zviad): test other APIs.
}
