package walle

import (
	"context"
	"encoding/hex"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/walle/wallelib"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	serverId string
	s        Storage
	c        wallelib.Client
}

func NewServer(serverId string, s Storage, c wallelib.Client) *Server {
	r := &Server{
		serverId: serverId,
		s:        s,
		c:        c,
	}
	return r
}

func (s *Server) NewWriter(
	ctx context.Context,
	req *walle_pb.NewWriterRequest) (*walle_pb.BaseResponse, error) {
	ss, ok := s.s.Stream(req.StreamUri)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "streamURI: %s not found", req.StreamUri)
	}
	ssTopology := ss.Topology()
	if req.TargetServerId == "" {
		return s.broadcastRequest(ctx, ssTopology.ServerIds,
			func(c walle_pb.WalleClient, serverId string) (*walle_pb.BaseResponse, error) {
				reqC := proto.Clone(req).(*walle_pb.NewWriterRequest)
				reqC.StreamVersion = ssTopology.Version
				reqC.TargetServerId = serverId
				return c.NewWriter(ctx, reqC)
			})
	}
	if !s.checkServerId(req.TargetServerId) {
		c, err := s.c.ForServerNoFallback(req.TargetServerId)
		if err != nil {
			return nil, err
		}
		return c.NewWriter(ctx, req)
	}
	if err := s.checkStreamVersion(req.StreamUri, req.StreamVersion, ss); err != nil {
		return nil, err
	}
	if err := s.checkAndUpdateWriterId(req.StreamUri, req.WriterId, ss); err != nil {
		return nil, err
	}
	return &walle_pb.BaseResponse{
		SuccessIds:    []string{s.serverId},
		StreamVersion: ssTopology.Version,
	}, nil
}

func (s *Server) LastEntry(
	ctx context.Context,
	req *walle_pb.LastEntryRequest) (*walle_pb.LastEntryResponse, error) {
	ss, ok := s.s.Stream(req.StreamUri)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "streamURI: %s not found", req.StreamUri)
	}
	if !s.checkServerId(req.TargetServerId) {
		c, err := s.c.ForServerNoFallback(req.TargetServerId)
		if err != nil {
			return nil, err
		}
		return c.LastEntry(ctx, req)
	}
	if err := s.checkStreamVersion(req.StreamUri, req.StreamVersion, ss); err != nil {
		return nil, err
	}
	entries := ss.LastEntry(req.IncludeUncommitted)
	return &walle_pb.LastEntryResponse{Entries: entries}, nil
}

func (s *Server) PutEntry(
	ctx context.Context,
	req *walle_pb.PutEntryRequest) (*walle_pb.BaseResponse, error) {
	ss, ok := s.s.Stream(req.StreamUri)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "streamURI: %s not found", req.StreamUri)
	}
	ssTopology := ss.Topology()
	if req.TargetServerId == "" {
		return s.broadcastRequest(ctx, ssTopology.ServerIds,
			func(c walle_pb.WalleClient, serverId string) (*walle_pb.BaseResponse, error) {
				reqC := proto.Clone(req).(*walle_pb.PutEntryRequest)
				reqC.StreamVersion = ssTopology.Version
				reqC.TargetServerId = serverId
				return c.PutEntry(ctx, reqC)
			})
	}
	if !s.checkServerId(req.TargetServerId) {
		c, err := s.c.ForServerNoFallback(req.TargetServerId)
		if err != nil {
			return nil, err
		}
		return c.PutEntry(ctx, req)
	}
	if err := s.checkStreamVersion(req.StreamUri, req.StreamVersion, ss); err != nil {
		return nil, err
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
	return &walle_pb.BaseResponse{
		SuccessIds:    []string{s.serverId},
		StreamVersion: ssTopology.Version,
	}, nil
}

func (s *Server) broadcastRequest(
	ctx context.Context,
	serverIds []string,
	call func(c walle_pb.WalleClient, serverId string) (*walle_pb.BaseResponse, error)) (*walle_pb.BaseResponse, error) {
	var successIds []string
	var errs []error
	var maxVersion int64
	// TODO(zviad): needs to be parallel/asynchronous
	for _, serverId := range serverIds {
		c, err := s.c.ForServerNoFallback(serverId)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		resp, err := call(c, serverId)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		successIds = append(successIds, serverId)
		if resp.StreamVersion > maxVersion {
			maxVersion = resp.StreamVersion
		}
	}
	if len(successIds) <= len(errs) {
		return nil, errors.Errorf("not enough success: %s <= %d\nerrs: %v", successIds, len(errs), errs)
	}
	return &walle_pb.BaseResponse{
		SuccessIds:    successIds,
		Fails:         int32(len(errs)),
		StreamVersion: maxVersion,
	}, nil
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

func (s *Server) checkStreamVersion(streamURI string, reqStreamVersion int64, ss StreamMetadata) error {
	version := ss.Topology().Version
	if reqStreamVersion == version-1 || reqStreamVersion == version || reqStreamVersion == version+1 {
		return nil
	}
	return errors.Errorf("stream[%s] incompatible version: %d vs %d", streamURI, reqStreamVersion, version)
}
