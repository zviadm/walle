package walle

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

type topoMgrClient struct {
	c wallelib.BasicClient
}

func NewTopoMgrClient(c wallelib.BasicClient) topomgr.TopoManagerClient {
	return &topoMgrClient{c}
}

func (t *topoMgrClient) connect(
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

func (t *topoMgrClient) FetchTopology(
	ctx context.Context, in *topomgr.FetchTopologyRequest, opts ...grpc.CallOption) (*walleapi.Topology, error) {
	conn, err := t.connect(ctx, in.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return topomgr.NewTopoManagerClient(conn).FetchTopology(ctx, in, opts...)
}
func (t *topoMgrClient) UpdateTopology(
	ctx context.Context, in *topomgr.UpdateTopologyRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	conn, err := t.connect(ctx, in.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return topomgr.NewTopoManagerClient(conn).UpdateTopology(ctx, in, opts...)
}
func (t *topoMgrClient) RegisterServer(
	ctx context.Context, in *topomgr.RegisterServerRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	conn, err := t.connect(ctx, in.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return topomgr.NewTopoManagerClient(conn).RegisterServer(ctx, in, opts...)
}
