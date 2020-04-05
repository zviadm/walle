package walle

import (
	"bytes"
	"context"
	"sort"
	"sync"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) broadcastWriterInfo(
	ctx context.Context, ss storage.Metadata) (*walle_pb.WriterInfoResponse, error) {
	ssTopology := ss.Topology()
	respMx := sync.Mutex{}
	var respMax *walle_pb.WriterInfoResponse
	var remainingMs []int64
	var streamVersions []int64
	_, err := s.broadcastRequest(ctx, ssTopology.ServerIds,
		func(c walle_pb.WalleClient, ctx context.Context, serverId string) error {
			resp, err := c.WriterInfo(ctx, &walle_pb.WriterInfoRequest{
				ServerId:      serverId,
				StreamUri:     ss.StreamURI(),
				StreamVersion: ssTopology.Version,
				FromServerId:  s.s.ServerId(),
			})
			respMx.Lock()
			defer respMx.Unlock()
			if bytes.Compare(resp.GetWriterId(), respMax.GetWriterId()) > 0 {
				respMax = resp
			}
			remainingMs = append(remainingMs, resp.GetRemainingLeaseMs())
			streamVersions = append(streamVersions, resp.GetStreamVersion())
			return err
		})
	if err != nil {
		return nil, err
	}
	respMx.Lock()
	defer respMx.Unlock()
	// Sort responses by (writerId, remainingLeaseMs) and choose one that majority is
	// greather than or equal to.
	sort.Slice(remainingMs, func(i, j int) bool { return remainingMs[i] < remainingMs[j] })
	sort.Slice(streamVersions, func(i, j int) bool { return streamVersions[i] < streamVersions[j] })
	respMax.RemainingLeaseMs = remainingMs[len(ssTopology.ServerIds)/2]
	respMax.StreamVersion = streamVersions[len(ssTopology.ServerIds)/2]
	return respMax, nil
}

// Broadcasts requests to all serverIds and returns list of serverIds that have succeeded.
// Returns an error if majority didn't succeed.
func (s *Server) broadcastRequest(
	ctx context.Context,
	serverIds []string,
	call func(
		c walle_pb.WalleClient,
		ctx context.Context,
		serverId string) error) (successIds []string, err error) {
	callCtx := ctx
	deadline, ok := ctx.Deadline()
	if ok {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithDeadline(context.Background(), deadline)
		defer func() {
			if err != nil {
				cancel()
			}
		}()
	}
	type callErr struct {
		ServerId string
		Err      error
	}
	errsC := make(chan *callErr, len(serverIds))
	for _, serverId := range serverIds {
		c, err := s.c.ForServer(serverId)
		if err != nil {
			errsC <- &callErr{ServerId: serverId, Err: err}
			continue
		}
		go func(c walle_pb.WalleClient, serverId string) {
			err := call(c, callCtx, serverId)
			errsC <- &callErr{ServerId: serverId, Err: err}
		}(c, serverId)
	}
	var errCodeFinal codes.Code
	var errs []error
	for i := 0; i < len(serverIds); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-errsC:
			if err.Err != nil {
				errs = append(errs, errors.Wrap(err.Err, err.ServerId))
				errCode := status.Convert(err.Err).Code()
				if wallelib.IsErrFinal(errCode) || !wallelib.IsErrFinal(errCodeFinal) {
					errCodeFinal = errCode
				}
			} else {
				successIds = append(successIds, err.ServerId)
			}
		}
		if len(successIds) >= len(serverIds)/2+1 {
			return successIds, nil
		}
		if len(errs) >= (len(serverIds)+1)/2 {
			return nil, status.Errorf(errCodeFinal, "errs: %d / %d - %s", len(errs), len(serverIds), errs)
		}
	}
	panic("unreachable code")
}
