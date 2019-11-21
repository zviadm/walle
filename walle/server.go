package walle

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/golang/glog"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	serverId string
	s        Storage
}

func NewServer(serverId string, s Storage) *Server {
	r := &Server{
		serverId: serverId,
		s:        s,
	}
	return r
}

func (s *Server) checkServerId(serverId string) bool {
	return serverId == s.serverId
}

// Checks writerId if it is still active for a given streamURI, and updates if necessary.
func (s *Server) checkAndUpdateWriterId(streamURI string, writerId string, ss StreamMetadata) error {
	ssWriterId := ss.WriterId()
	if writerId < ssWriterId {
		return status.Errorf(codes.FailedPrecondition, "writer no longer active: %s < %s", writerId, ssWriterId)
	}
	// TODO(zviad): update heartbeat for `writerIds`.

	if writerId == ssWriterId {
		return nil
	}
	glog.Infof(
		"[%s] writerId: updating %s -> %s",
		streamURI, hex.EncodeToString([]byte(ssWriterId)), hex.EncodeToString([]byte(writerId)))
	ss.UpdateWriterId(writerId)
	return nil
}

func (s *Server) NewWriter(
	ctx context.Context,
	req *walle_pb.NewWriterRequest) (*walle_pb.BaseResponse, error) {
	if req.TargetServerId == "" {
		// TODO(zviad): Forward request.
		return nil, errors.New("not implemented")
	}
	if !s.checkServerId(req.TargetServerId) {
		// TODO(zviad): Forward request instead.
		return nil, errors.New("not implemented")
	}
	ss, ok := s.s.Stream(req.StreamUri)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "streamURI: %s not found", req.StreamUri)
	}
	if err := s.checkAndUpdateWriterId(req.StreamUri, req.WriterId, ss); err != nil {
		return nil, err
	}
	return &walle_pb.BaseResponse{SuccessIds: []string{s.serverId}}, nil
}

func (s *Server) LastEntry(
	ctx context.Context,
	req *walle_pb.LastEntryRequest) (*walle_pb.LastEntryResponse, error) {
	if !s.checkServerId(req.TargetServerId) {
		// TODO(zviad): Forward request instead.
		return nil, errors.New("not implemented")
	}
	ss, ok := s.s.Stream(req.StreamUri)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "streamURI: %s not found", req.StreamUri)
	}
	entries := ss.LastEntry(req.IncludeUncommitted)
	return &walle_pb.LastEntryResponse{Entries: entries}, nil
}

func (s *Server) PutEntry(
	ctx context.Context,
	req *walle_pb.PutEntryRequest) (*walle_pb.BaseResponse, error) {
	if req.TargetServerId == "" {
		// TODO(zviad): Forward request.
		return nil, errors.New("not implemented")
	}
	if !s.checkServerId(req.TargetServerId) {
		// TODO(zviad): Forward request instead.
		return nil, errors.New("not implemented")
	}
	ss, ok := s.s.Stream(req.StreamUri)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "streamURI: %s not found", req.StreamUri)
	}
	if err := s.checkAndUpdateWriterId(req.StreamUri, req.Entry.WriterId, ss); err != nil {
		return nil, err
	}

	if req.Entry.EntryId == 0 || req.Entry.EntryId > req.CommittedEntryId {
		// Perform commit first. If commit can't happen, there is no point in trying to perform the put.
		ok := ss.CommitEntry(req.CommittedEntryId, req.CommittedEntryMd5)
		if !ok {
			return nil, status.Errorf(codes.OutOfRange, "TODO(zviad): error message")
		}
	}
	if req.Entry.EntryId == 0 {
		return &walle_pb.BaseResponse{SuccessIds: []string{s.serverId}}, nil
	}
	isCommitted := req.CommittedEntryId >= req.Entry.EntryId
	ok = ss.PutEntry(req.Entry, isCommitted)
	// TODO(zviad): Should we wait a bit before letting client retry?
	if !ok {
		return nil, status.Errorf(codes.OutOfRange, "TODO(zviad): error message")
	}
	return &walle_pb.BaseResponse{SuccessIds: []string{s.serverId}}, nil
}
