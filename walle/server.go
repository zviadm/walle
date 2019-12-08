package walle

import (
	"context"
	"encoding/hex"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	serverId string
	s        Storage
	c        Client
}

type Client interface {
	ForServer(serverId string) (walle_pb.WalleClient, error)
}

func NewServer(serverId string, s Storage, c Client) *Server {
	r := &Server{
		serverId: serverId,
		s:        s,
		c:        c,
	}
	return r
}

func (s *Server) NewWriter(
	ctx context.Context,
	req *walle_pb.NewWriterRequest) (*walle_pb.NewWriterResponse, error) {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return nil, err
	}
	if err := s.checkAndUpdateWriterId(req.StreamUri, req.WriterId, ss); err != nil {
		return nil, err
	}
	return &walle_pb.NewWriterResponse{}, nil
}

func (s *Server) PutEntryInternal(
	ctx context.Context,
	req *walle_pb.PutEntryInternalRequest) (*walle_pb.PutEntryInternalResponse, error) {
	ss, err := s.processRequestHeader(req)
	if err != nil {
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
		return &walle_pb.PutEntryInternalResponse{}, nil
	}
	isCommitted := req.CommittedEntryId >= req.Entry.EntryId
	ok := ss.PutEntry(req.Entry, isCommitted)
	// TODO(zviad): Should we wait a bit before letting client retry?
	if !ok {
		return nil, status.Errorf(codes.OutOfRange, "TODO(zviad): error message")
	}
	return &walle_pb.PutEntryInternalResponse{}, nil
}

func (s *Server) LastEntries(
	ctx context.Context,
	req *walle_pb.LastEntriesRequest) (*walle_pb.LastEntriesResponse, error) {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return nil, err
	}
	entries := ss.LastEntries()
	return &walle_pb.LastEntriesResponse{Entries: entries}, nil
}

func (s *Server) ReadEntries(
	req *walle_pb.ReadEntriesRequest, stream walle_pb.Walle_ReadEntriesServer) error {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return err
	}
	entryId := req.StartEntryId
	cursor := ss.ReadFrom(entryId)
	for entryId < req.EndEntryId {
		entry, ok := cursor.Next()
		if !ok {
			return status.Error(codes.NotFound, "reached end of the stream")
		}
		if entry.EntryId != entryId {
			return status.Errorf(codes.NotFound, "entry: %d is missing", entryId)
		}
		entryId += 1
		err := stream.Send(entry)
		if err != nil {
			return err
		}
	}
	return nil
}

type requestHeader interface {
	GetServerId() string
	GetStreamUri() string
	GetStreamVersion() int64
}

func (s *Server) processRequestHeader(req requestHeader) (ss StreamStorage, err error) {
	if !s.checkServerId(req.GetServerId()) {
		return nil, status.Errorf(codes.NotFound, "invalid serverId: %s", req.GetServerId())
	}
	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "streamURI: %s not found", req.GetStreamUri())
	}
	if err := s.checkStreamVersion(req.GetStreamUri(), req.GetStreamVersion(), ss); err != nil {
		return nil, err
	}
	return ss, nil
}

func (s *Server) checkServerId(serverId string) bool {
	return serverId == s.serverId
}

func (s *Server) checkStreamVersion(streamURI string, reqStreamVersion int64, ss StreamMetadata) error {
	version := ss.Topology().Version
	if reqStreamVersion == version-1 || reqStreamVersion == version || reqStreamVersion == version+1 {
		return nil
	}
	return errors.Errorf("stream[%s] incompatible version: %d vs %d", streamURI, reqStreamVersion, version)
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
