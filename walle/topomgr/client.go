package topomgr

import (
	"context"
	"time"

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

// NewClient returns client that can be used to talk to TopoManagerServer.
func NewClient(root wallelib.Client) topomgr.TopoManagerClient {
	return &client{root}
}

func (t *client) connectAndDo(
	ctx context.Context,
	clusterURI string,
	f func(ctx context.Context, c topomgr.TopoManagerClient) error) (err error) {
	callTimeout := 2 * time.Second
	ctx, cancel := context.WithTimeout(ctx, 5*callTimeout)
	defer cancel()
	return wallelib.KeepTryingWithBackoff(
		ctx, wallelib.LeaseMinimum, callTimeout,
		func(retryN uint) (bool, bool, error) {
			ctx, cancel := context.WithTimeout(ctx, callTimeout)
			defer cancel()
			cli, err := t.c.ForStream(clusterURI)
			if err != nil {
				return false, false, err
			}
			wStatus, err := cli.WriterStatus(
				ctx, &walleapi.WriterStatusRequest{StreamUri: clusterURI})
			if err != nil {
				return false, false, err
			}
			if wStatus.RemainingLeaseMs <= 0 {
				return false, false, status.Errorf(codes.Unavailable, "no active manager for: %s", clusterURI)
			}
			conn, err := grpc.DialContext(ctx, wStatus.WriterAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				return false, false, err
			}
			defer conn.Close()
			err = f(ctx, topomgr.NewTopoManagerClient(conn))
			errCode := status.Convert(err).Code()
			errFinal := errCode == codes.OK || wallelib.IsErrFinal(errCode)
			return errFinal, false, err
		})
}

func (t *client) FetchTopology(
	ctx context.Context, in *topomgr.FetchTopologyRequest, opts ...grpc.CallOption) (r *walleapi.Topology, err error) {
	err = t.connectAndDo(ctx, in.ClusterUri,
		func(ctx context.Context, c topomgr.TopoManagerClient) error {
			r, err = c.FetchTopology(ctx, in, opts...)
			return err
		})
	return r, err
}
func (t *client) RegisterServer(
	ctx context.Context, in *topomgr.RegisterServerRequest, opts ...grpc.CallOption) (r *walleapi.Empty, err error) {
	err = t.connectAndDo(ctx, in.ClusterUri,
		func(ctx context.Context, c topomgr.TopoManagerClient) error {
			r, err = c.RegisterServer(ctx, in, opts...)
			return err
		})
	return r, err
}
func (t *client) CrUpdateStream(
	ctx context.Context, in *topomgr.CrUpdateStreamRequest, opts ...grpc.CallOption) (r *walleapi.Empty, err error) {
	err = t.connectAndDo(ctx, in.ClusterUri,
		func(ctx context.Context, c topomgr.TopoManagerClient) error {
			r, err = c.CrUpdateStream(ctx, in, opts...)
			return err
		})
	return r, err
}
