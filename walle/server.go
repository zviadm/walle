package walle

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	rootCtx context.Context
	s       Storage
	c       Client

	topoMgr *topomgr.Manager
}

type Client interface {
	wallelib.BasicClient
	ForServer(serverId string) (walle_pb.WalleClient, error)
}

func NewServer(
	ctx context.Context,
	s Storage,
	c Client,
	d wallelib.Discovery,
	topoMgr *topomgr.Manager) *Server {
	r := &Server{
		rootCtx: ctx,
		s:       s,
		c:       c,
	}

	r.watchTopology(ctx, d, topoMgr)
	go r.writerInfoWatcher(ctx)
	go r.gapHandler(ctx)
	return r
}

func (s *Server) NewWriter(
	ctx context.Context,
	req *walle_pb.NewWriterRequest) (*walle_pb.NewWriterResponse, error) {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return nil, err
	}
	reqWriterId := WriterId(req.WriterId)
	ok, remainingLease := ss.UpdateWriter(reqWriterId, req.WriterAddr, time.Duration(req.LeaseMs)*time.Millisecond)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "there is already another new writer")
	}

	// Need to wait `remainingLease` duration before returning. However, we also need to make sure new
	// lease doesn't expire since writer client can't heartbeat until this call succeeds.
	sleepTill := time.Now().Add(remainingLease)
	iterSleep := time.Duration(req.LeaseMs) * time.Millisecond / 10
	if iterSleep > 0 {
		iterN := int(remainingLease / iterSleep)
		for idx := 0; idx < iterN; idx++ {
			time.Sleep(iterSleep)
			ss.RenewLease(reqWriterId)
		}
	}
	if finalSleep := sleepTill.Sub(time.Now()); finalSleep > 0 {
		time.Sleep(finalSleep)
		ss.RenewLease(reqWriterId)
	}
	return &walle_pb.NewWriterResponse{}, nil
}

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

func (s *Server) PutEntryInternal(
	ctx context.Context,
	req *walle_pb.PutEntryInternalRequest) (*walle_pb.PutEntryInternalResponse, error) {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return nil, err
	}
	if err := s.checkAndUpdateWriterId(ctx, ss, WriterId(req.Entry.WriterId)); err != nil {
		return nil, err
	}
	if req.Entry.EntryId == 0 || req.Entry.EntryId > req.CommittedEntryId {
		// Perform commit first. If commit can't happen, there is no point in trying to perform the put.
		ok := ss.CommitEntry(req.CommittedEntryId, req.CommittedEntryMd5)
		if !ok {
			// Try to fetch the committed entry from other servers and create a GAP locally to continue with
			// the put.
			// TODO(zviad): Should we first wait/retry CommitEntry? Maybe PutEntry calls are on the way already.
			err := s.fetchAndStoreEntries(ctx, ss, req.CommittedEntryId, req.CommittedEntryId+1, nil)
			if err != nil {
				return nil, status.Errorf(codes.OutOfRange, "commit entryId: %d - %s", req.CommittedEntryId, err)
			}
			zlog.Infof("[%s] commit caught up to: %d (might have created a gap)", ss.StreamURI(), req.CommittedEntryId)
		}
	}
	if req.Entry.EntryId == 0 {
		return &walle_pb.PutEntryInternalResponse{}, nil
	}
	isCommitted := req.CommittedEntryId >= req.Entry.EntryId
	ok := ss.PutEntry(req.Entry, isCommitted)
	// TODO(zviad): Should we wait a bit before letting client retry?
	if !ok {
		return nil, status.Errorf(codes.OutOfRange, "put entryId: %d, commitId: %d", req.Entry.EntryId, req.CommittedEntryId)
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
	defer cursor.Close()
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
	ss, ok := s.s.Stream(req.GetStreamUri(), true)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "streamURI: %s not found locally", req.GetStreamUri())
	}
	if err := s.checkStreamVersion(ss, req.GetStreamVersion()); err != nil {
		return nil, err
	}
	return ss, nil
}

func (s *Server) checkServerId(serverId string) bool {
	return serverId == s.s.ServerId()
}

func (s *Server) checkStreamVersion(ss StreamMetadata, reqStreamVersion int64) error {
	version := ss.Topology().Version
	if reqStreamVersion == version-1 || reqStreamVersion == version || reqStreamVersion == version+1 {
		return nil
	}
	return errors.Errorf("stream[%s] incompatible version: %d vs %d", ss.StreamURI(), reqStreamVersion, version)
}

// Checks writerId if it is still active for a given streamURI. If newer writerId is supplied, will try
// to get updated information from other servers because this server must have missed the NewWriter call.
func (s *Server) checkAndUpdateWriterId(ctx context.Context, ss StreamMetadata, writerId WriterId) error {
	for {
		ssWriterId, _, _, _ := ss.WriterInfo()
		if writerId == ssWriterId {
			ss.RenewLease(writerId)
			return nil
		}
		if writerId < ssWriterId {
			return status.Errorf(codes.FailedPrecondition, "writer no longer active: %s < %s", writerId, ssWriterId)
		}
		resp, err := s.broadcastWriterInfo(ctx, ss)
		if err != nil {
			return err
		}
		respWriterId := WriterId(resp.WriterId)
		if respWriterId <= ssWriterId {
			return status.Errorf(codes.Internal, "writerId is newer than majority?: %s > %s", writerId, ssWriterId)
		}
		zlog.Infof(
			"[%s] writerId: updating %s -> %s",
			ss.StreamURI(), hex.EncodeToString([]byte(ssWriterId)), hex.EncodeToString([]byte(writerId)))
		ss.UpdateWriter(respWriterId, resp.WriterAddr, time.Duration(resp.LeaseMs)*time.Millisecond)
	}
}
