package topomgr

import (
	"context"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
	"google.golang.org/grpc"
)

type client struct {
	c wallelib.BasicClient
}

func NewClient(c wallelib.BasicClient) topomgr.TopoManagerClient {
	return &client{c}
}

func (t *client) connect(
	ctx context.Context,
	topologyURI string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	err := wallelib.KeepTryingWithBackoff(
		ctx, wallelib.LeaseMinimum, time.Second,
		func(retryN uint) (bool, error) {
			cli, err := t.c.ForStream(topologyURI)
			if err != nil {
				return false, err
			}
			status, err := cli.WriterStatus(
				ctx, &walleapi.WriterStatusRequest{StreamUri: topologyURI})
			if err != nil {
				return false, err
			}
			if status.RemainingLeaseMs <= 0 {
				return false, errors.Errorf("no active manager for: %s", topologyURI)
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			conn, err = grpc.DialContext(ctx, status.WriterAddr, grpc.WithInsecure(), grpc.WithBlock())
			return err == nil, err
		})
	return conn, err
}

func (t *client) FetchTopology(
	ctx context.Context, in *topomgr.FetchTopologyRequest, opts ...grpc.CallOption) (*walleapi.Topology, error) {
	conn, err := t.connect(ctx, in.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return topomgr.NewTopoManagerClient(conn).FetchTopology(ctx, in, opts...)
}
func (t *client) UpdateTopology(
	ctx context.Context, in *topomgr.UpdateTopologyRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	conn, err := t.connect(ctx, in.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return topomgr.NewTopoManagerClient(conn).UpdateTopology(ctx, in, opts...)
}
func (t *client) UpdateServerInfo(
	ctx context.Context, in *topomgr.UpdateServerInfoRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	conn, err := t.connect(ctx, in.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return topomgr.NewTopoManagerClient(conn).UpdateServerInfo(ctx, in, opts...)
}
func (t *client) UpdateServerIds(
	ctx context.Context, in *topomgr.UpdateServerIdsRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	conn, err := t.connect(ctx, in.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return topomgr.NewTopoManagerClient(conn).UpdateServerIds(ctx, in, opts...)
}
