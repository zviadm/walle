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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type client struct {
	c wallelib.BasicClient
}

func NewClient(c wallelib.BasicClient) topomgr.TopoManagerClient {
	return &client{c}
}

func (t *client) connectAndDo(
	ctx context.Context,
	topologyURI string,
	f func(ctx context.Context, c topomgr.TopoManagerClient) error) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return wallelib.KeepTryingWithBackoff(
		ctx, wallelib.LeaseMinimum, time.Second,
		func(retryN uint) (bool, error) {
			cli, err := t.c.ForStream(topologyURI)
			if err != nil {
				return false, err
			}
			wStatus, err := cli.WriterStatus(
				ctx, &walleapi.WriterStatusRequest{StreamUri: topologyURI})
			if err != nil {
				return false, err
			}
			if wStatus.RemainingLeaseMs <= 0 {
				return false, errors.Errorf("no active manager for: %s", topologyURI)
			}
			ctxConn, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			conn, err := grpc.DialContext(ctxConn, wStatus.WriterAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				return false, err
			}
			defer conn.Close()
			err = f(ctx, topomgr.NewTopoManagerClient(conn))
			errStatus, _ := status.FromError(err)
			errFinal := errStatus.Code() == codes.OK || errStatus.Code() == codes.FailedPrecondition
			return errFinal, err
		})
}

func (t *client) FetchTopology(
	ctx context.Context, in *topomgr.FetchTopologyRequest, opts ...grpc.CallOption) (r *walleapi.Topology, err error) {
	err = t.connectAndDo(ctx, in.TopologyUri,
		func(ctx context.Context, c topomgr.TopoManagerClient) error {
			r, err = c.FetchTopology(ctx, in, opts...)
			return err
		})
	return r, err
}
func (t *client) UpdateServerInfo(
	ctx context.Context, in *topomgr.UpdateServerInfoRequest, opts ...grpc.CallOption) (r *empty.Empty, err error) {
	err = t.connectAndDo(ctx, in.TopologyUri,
		func(ctx context.Context, c topomgr.TopoManagerClient) error {
			r, err = c.UpdateServerInfo(ctx, in, opts...)
			return err
		})
	return r, err
}
func (t *client) UpdateServerIds(
	ctx context.Context, in *topomgr.UpdateServerIdsRequest, opts ...grpc.CallOption) (r *topomgr.UpdateServerIdsResponse, err error) {
	err = t.connectAndDo(ctx, in.TopologyUri,
		func(ctx context.Context, c topomgr.TopoManagerClient) error {
			r, err = c.UpdateServerIds(ctx, in, opts...)
			return err
		})
	return r, err
}
