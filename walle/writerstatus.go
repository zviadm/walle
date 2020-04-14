package walle

import (
	"context"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/broadcast"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WriterStatus implements WalleApiServer interface.
func (s *Server) WriterStatus(
	ctx context.Context,
	req *walleapi.WriterStatusRequest) (*walleapi.WriterStatusResponse, error) {
	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%s not found", req.GetStreamUri())
	}

	writerInfo, err := broadcast.WriterInfo(
		ctx, s.c, s.s.ServerId(), ss.StreamURI(), ss.Topology())
	if err != nil {
		return nil, err
	}
	return &walleapi.WriterStatusResponse{
		WriterAddr:       writerInfo.WriterAddr,
		LeaseMs:          writerInfo.LeaseMs,
		RemainingLeaseMs: writerInfo.RemainingLeaseMs,
		StreamVersion:    writerInfo.StreamVersion,
	}, nil
}

// WriterInfo implements WalleServer interface.
func (s *Server) WriterInfo(
	ctx context.Context,
	req *walle_pb.WriterInfoRequest) (*walle_pb.WriterInfoResponse, error) {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return nil, err
	}
	writerId, writerAddr, lease, remainingLease := ss.WriterInfo()
	return &walle_pb.WriterInfoResponse{
		WriterId:         writerId.Encode(),
		WriterAddr:       writerAddr,
		LeaseMs:          lease.Nanoseconds() / time.Millisecond.Nanoseconds(),
		RemainingLeaseMs: remainingLease.Nanoseconds() / time.Millisecond.Nanoseconds(),
		StreamVersion:    ss.Topology().Version,
	}, nil
}
