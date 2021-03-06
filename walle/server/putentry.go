package server

import (
	"context"
	"time"

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

var emptyOk = &walleapi.Empty{}

// PutEntry implements WalleApiServer interface.
func (s *Server) PutEntry(
	ctx context.Context, req *walleapi.PutEntryRequest) (*walleapi.PutEntryResponse, error) {
	if req.Entry == nil {
		return nil, status.Errorf(codes.InvalidArgument, "req.Entry must be set for WriterId")
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
	serverIds, err := broadcast.Call(ctx, s.c, ssTopology.ServerIds, putEntryLiveWait, putEntryBgWait,
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
	return &walleapi.PutEntryResponse{ServerIds: serverIds}, nil
}

// PutEntryInternal implements WalleServer interface.
func (s *Server) PutEntryInternal(
	ctx context.Context,
	req *walle_pb.PutEntryInternalRequest) (resp *walleapi.Empty, err error) {
	defer func() {
		if err == context.Canceled {
			err = status.Error(codes.Canceled, "put canceled by broadcast")
		} else if err != nil {
			errCode := status.Convert(err).Code()
			if errCode == codes.Internal || errCode == codes.DataLoss {
				zlog.Errorf("%s: %s", req.StreamUri, err)
			}
		}
	}()
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return nil, err
	}
	defer s.inflightReqs.Add(-1)
	writerId := req.Entry.WriterId
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
			if err := s.s.Flush(ctx); err != nil {
				return nil, err
			}
		}
		return &walleapi.Empty{}, nil
	}
}

// Checks writerId if it is still active for a given streamURI. If newer writerId is supplied, will try
// to get updated information from other servers because this server must have missed the NewWriter call.
func (s *Server) checkAndUpdateWriterId(
	ctx context.Context,
	ss storage.Metadata,
	writerId walleapi.WriterId) error {
	ssWriterId, writerAddr, _, _ := ss.WriterInfo()
	cmpWriterId := storage.CmpWriterIds(writerId, ssWriterId)
	if cmpWriterId == 0 {
		return nil
	}
	if cmpWriterId < 0 {
		return status.Errorf(
			codes.FailedPrecondition, "writer no longer active: %v < %v (%s)", writerId, ssWriterId, writerAddr)
	}
	// Slow path. Make sure there is only one go routine doing the fetching at a time.
	s.fetchWriterIdMX.Lock()
	defer s.fetchWriterIdMX.Unlock()
	ssWriterId, writerAddr, _, _ = ss.WriterInfo()
	cmpWriterId = storage.CmpWriterIds(writerId, ssWriterId)
	if cmpWriterId == 0 {
		return nil
	}
	if cmpWriterId < 0 {
		return status.Errorf(
			codes.FailedPrecondition, "writer no longer active: %v < %v (%s)", writerId, ssWriterId, writerAddr)
	}
	resp, err := broadcast.WriterInfo(ctx, s.c, s.s.ServerId(), ss.StreamURI(), ss.Topology())
	if err != nil {
		return err
	}
	if storage.CmpWriterIds(resp.WriterId, ssWriterId) <= 0 {
		return status.Errorf(codes.Internal, "writerId is newer than majority?: %v > %v", writerId, ssWriterId)
	}
	zlog.Infof(
		"[%s] writerId update: %s %v -> %s %v",
		ss.StreamURI(), writerAddr, ssWriterId, resp.WriterAddr, resp.WriterId)
	ss.UpdateWriter(resp.WriterId, resp.WriterAddr, time.Duration(resp.LeaseMs)*time.Millisecond)
	return nil
}
