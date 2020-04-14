package walle

import (
	"bytes"
	"context"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/zviadm/stats-go/metrics"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/broadcast"
	"github.com/zviadm/walle/walle/pipeline"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Broadcast for PutEntry calls will wait `putEntryLiveWait` amount of time for all calls to finish
	// within request itself. And will continue waiting upto `putEntryBgWait` amount of time in the background
	// before canceling context. This is useful to make sure live healthy servers aren't falling too far behind.
	putEntryLiveWait = 10 * time.Millisecond
	putEntryBgWait   = pipeline.QueueMaxTimeout
)

var emptyOk = &empty.Empty{}

// PutEntry implements WalleApiServer interface.
func (s *Server) PutEntry(
	ctx context.Context, req *walleapi.PutEntryRequest) (*empty.Empty, error) {
	if len(req.Entry.GetWriterId()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "writer_id must be set")
	}
	if len(req.Entry.Data) > wallelib.MaxEntrySize {
		return nil, status.Errorf(
			codes.InvalidArgument, "entry too large: %d > %d", len(req.Entry.Data), wallelib.MaxEntrySize)
	}

	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%s not found", req.GetStreamUri())
	}
	ssTopology := ss.Topology()
	_, err := broadcast.Call(ctx, s.c, ssTopology.ServerIds, putEntryLiveWait, putEntryBgWait,
		func(c walle_pb.WalleClient, ctx context.Context, serverId string) error {
			_, err := c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
				ServerId:         serverId,
				StreamUri:        req.StreamUri,
				StreamVersion:    ssTopology.Version,
				FromServerId:     s.s.ServerId(),
				Entry:            req.Entry,
				CommittedEntryId: req.CommittedEntryId,
				CommittedEntryXX: req.CommittedEntryXX,
			})
			return err
		})
	if err != nil {
		return nil, err
	}
	return emptyOk, nil
}

// PutEntryInternal implements WalleServer interface.
func (s *Server) PutEntryInternal(
	ctx context.Context,
	req *walle_pb.PutEntryInternalRequest) (*walle_pb.PutEntryInternalResponse, error) {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return nil, err
	}
	writerId := storage.WriterId(req.Entry.WriterId)
	isCommitted := req.CommittedEntryId >= req.Entry.EntryId
	err = s.checkAndUpdateWriterId(ctx, ss, writerId)
	if err != nil && (!req.IgnoreLeaseRenew || !isCommitted) {
		return nil, err
	}
	err = ss.RenewLease(writerId, 0)
	if err != nil && !req.IgnoreLeaseRenew {
		return nil, err
	}
	if req.Entry.EntryId == 0 {
		heartbeatsCounter.V(metrics.KV{"stream_uri": ss.StreamURI()}).Count(1)
	}

	p := s.pipeline.ForStream(ss)
	var res *pipeline.ResultCtx
	if req.Entry.EntryId == 0 || !isCommitted {
		res = p.QueueCommit(req.CommittedEntryId, req.CommittedEntryXX)
	}
	if req.Entry.EntryId > 0 {
		res = p.QueuePut(req.Entry, isCommitted)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-res.Done():
		if err := res.Err(); err != nil {
			return nil, err
		}
		if req.Entry.EntryId > 0 {
			// Every successful PutEntry call that might have written actual data, requires
			// flush. Commit only entries don't require flushes because they won't cause
			// data loss.
			if err := s.pipeline.Flush(ctx); err != nil {
				return nil, err
			}
		}
		return &walle_pb.PutEntryInternalResponse{}, nil
	}
}

// Checks writerId if it is still active for a given streamURI. If newer writerId is supplied, will try
// to get updated information from other servers because this server must have missed the NewWriter call.
func (s *Server) checkAndUpdateWriterId(
	ctx context.Context,
	ss storage.Metadata,
	writerId storage.WriterId) error {
	for {
		ssWriterId, writerAddr, _, _ := ss.WriterInfo()
		cmpWriterId := bytes.Compare(writerId, ssWriterId)
		if cmpWriterId == 0 {
			return nil
		}
		if cmpWriterId < 0 {
			return status.Errorf(
				codes.FailedPrecondition, "writer no longer active: %s < %s (%s)", writerId, ssWriterId, writerAddr)
		}
		resp, err := broadcast.WriterInfo(ctx, s.c, s.s.ServerId(), ss.StreamURI(), ss.Topology())
		if err != nil {
			return err
		}
		respWriterId := storage.WriterId(resp.WriterId)
		if bytes.Compare(respWriterId, ssWriterId) <= 0 {
			return status.Errorf(codes.Internal, "writerId is newer than majority?: %s > %s", writerId, ssWriterId)
		}
		zlog.Infof(
			"[%s] writerId update: %s (%s) -> %s (%s)",
			ss.StreamURI(), writerAddr, ssWriterId, resp.WriterAddr, respWriterId)
		ss.UpdateWriter(respWriterId, resp.WriterAddr, time.Duration(resp.LeaseMs)*time.Millisecond)
	}
}
