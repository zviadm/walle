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
	c wallelib.Client
}

func NewClient(root wallelib.Client) topomgr.TopoManagerClient {
	return &client{root}
}

func (t *client) connectAndDo(
	ctx context.Context,
	topologyURI string,
	f func(ctx context.Context, c topomgr.TopoManagerClient) error) (err error) {
	callTimeout := 2 * time.Second
	ctx, cancel := context.WithTimeout(ctx, 5*callTimeout)
	defer cancel()
	return wallelib.KeepTryingWithBackoff(
		ctx, wallelib.LeaseMinimum, callTimeout,
		func(retryN uint) (bool, bool, error) {
			ctx, cancel := context.WithTimeout(ctx, callTimeout)
			defer cancel()
			cli, err := t.c.ForStream(topologyURI)
			if err != nil {
				return false, false, err
			}
			wStatus, err := cli.WriterStatus(
				ctx, &walleapi.WriterStatusRequest{StreamUri: topologyURI})
			if err != nil {
				return false, false, err
			}
			if wStatus.RemainingLeaseMs <= 0 {
				return false, false, errors.Errorf("no active manager for: %s", topologyURI)
			}
			conn, err := grpc.DialContext(ctx, wStatus.WriterAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				return false, false, err
			}
			defer conn.Close()
			err = f(ctx, topomgr.NewTopoManagerClient(conn))
			errCode := status.Convert(err).Code()
			errFinal := errCode == codes.OK || errCode == codes.FailedPrecondition
			return errFinal, false, err
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
