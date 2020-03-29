package wallelib

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	. "github.com/zviadm/walle/proto/walleapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errMinBackoffNs      = LeaseMinimum.Nanoseconds()
	errMaxBackoffNs      = time.Second.Nanoseconds()
	errBackoffMultiplier = 1.6

	maxErrN = math.Log(float64(errMaxBackoffNs/errMinBackoffNs)) / math.Log(errBackoffMultiplier)
)

// Wraps WalleApiClient to provide custom error handling for handling bad
// server nodes.
type wApiClient struct {
	cli      WalleApiClient
	errN     *int64
	downNano *int64
}

func (c *wApiClient) handleCallErr(err error) {
	code := status.Convert(err).Code()
	if code != codes.DeadlineExceeded &&
		code != codes.Unavailable &&
		code != codes.NotFound &&
		code != codes.Unknown {
		atomic.StoreInt64(c.errN, 0)
		return
	}
	errN := float64(atomic.AddInt64(c.errN, 1))
	if errN > maxErrN {
		errN = maxErrN
	}
	downNano := time.Now().UnixNano() + errMinBackoffNs*int64(math.Pow(errBackoffMultiplier, errN))
	atomic.StoreInt64(c.downNano, downNano)
}

func (c *wApiClient) ClaimWriter(ctx context.Context, in *ClaimWriterRequest, opts ...grpc.CallOption) (*ClaimWriterResponse, error) {
	r, err := c.cli.ClaimWriter(ctx, in, opts...)
	c.handleCallErr(err)
	return r, err
}
func (c *wApiClient) WriterStatus(ctx context.Context, in *WriterStatusRequest, opts ...grpc.CallOption) (*WriterStatusResponse, error) {
	r, err := c.cli.WriterStatus(ctx, in, opts...)
	c.handleCallErr(err)
	return r, err
}
func (c *wApiClient) PutEntry(ctx context.Context, in *PutEntryRequest, opts ...grpc.CallOption) (*PutEntryResponse, error) {
	r, err := c.cli.PutEntry(ctx, in, opts...)
	// Slight HaX: only worry about errors from heartbeater calls. Regular PutEntry calls can timeout
	// due to overload and other issues, and since there can be many PutEntry calls in-flight at the same time
	// it can cause issues with marking nodes bad unnecessarily.
	if in.GetEntry().GetEntryId() == 0 {
		c.handleCallErr(err)
	}
	return r, err
}
func (c *wApiClient) StreamEntries(ctx context.Context, in *StreamEntriesRequest, opts ...grpc.CallOption) (WalleApi_StreamEntriesClient, error) {
	r, err := c.cli.StreamEntries(ctx, in, opts...)
	c.handleCallErr(err)
	return r, err
}
