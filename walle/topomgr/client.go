package topomgr

import (
	"context"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
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

func (t *client) connectAndDo(
	ctx context.Context,
	topologyURI string,
	f func(c topomgr.TopoManagerClient) error) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return wallelib.KeepTryingWithBackoff(
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
			conn, err := grpc.DialContext(ctx, status.WriterAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				return false, err
			}
			defer conn.Close()
			err = f(topomgr.NewTopoManagerClient(conn))
			return err == nil, err
		})
}

func (t *client) FetchTopology(
	ctx context.Context, in *topomgr.FetchTopologyRequest, opts ...grpc.CallOption) (r *walleapi.Topology, err error) {
	err = t.connectAndDo(ctx, in.TopologyUri,
		func(c topomgr.TopoManagerClient) error {
			r, err = c.FetchTopology(ctx, in, opts...)
			return err
		})
	return r, err
}
func (t *client) UpdateServerInfo(
	ctx context.Context, in *topomgr.UpdateServerInfoRequest, opts ...grpc.CallOption) (r *empty.Empty, err error) {
	err = t.connectAndDo(ctx, in.TopologyUri,
		func(c topomgr.TopoManagerClient) error {
			r, err = c.UpdateServerInfo(ctx, in, opts...)
			return err
		})
	return r, err
}
func (t *client) UpdateServerIds(
	ctx context.Context, in *topomgr.UpdateServerIdsRequest, opts ...grpc.CallOption) (r *topomgr.UpdateServerIdsResponse, err error) {
	err = t.connectAndDo(ctx, in.TopologyUri,
		func(c topomgr.TopoManagerClient) error {
			r, err = c.UpdateServerIds(ctx, in, opts...)
			return err
		})
	return r, err
}
