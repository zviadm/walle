package walle

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/pipeline"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	rootCtx context.Context
	s       storage.Storage
	c       Client

	pipeline *pipeline.Pipeline
	topoMgr  *topomgr.Manager
}

type Client interface {
	wallelib.BasicClient
	ForServer(serverId string) (walle_pb.WalleClient, error)
}

func NewServer(
	ctx context.Context,
	s storage.Storage,
	c Client,
	d wallelib.Discovery,
	topoMgr *topomgr.Manager) *Server {
	r := &Server{
		rootCtx: ctx,
		s:       s,
		c:       c,
	}
	r.pipeline = pipeline.New(ctx, s.FlushSync, r.fetchCommittedEntry)

	r.watchTopology(ctx, d, topoMgr)
	go r.writerInfoWatcher(ctx)
	go r.gapHandler(ctx)

	// Renew all writer leases at startup.
	for _, streamURI := range s.Streams(true) {
		ss, ok := s.Stream(streamURI, true)
		if !ok {
			continue
		}
		writerId, _, _, _ := ss.WriterInfo()
		ss.RenewLease(writerId)
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
	reqWriterId := storage.WriterId(req.WriterId)
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
	finalSleep := sleepTill.Sub(time.Now())
	time.Sleep(finalSleep)
	ss.RenewLease(reqWriterId)
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
	writerId := storage.WriterId(req.Entry.WriterId)
	if err := s.checkAndUpdateWriterId(ctx, ss, writerId); err != nil {
		return nil, err
	}
	ss.RenewLease(writerId)

	p := s.pipeline.ForStream(ss)
	var okC <-chan bool
	if req.Entry.EntryId == 0 || req.Entry.EntryId > req.CommittedEntryId {
		if ss.CommitEntry(req.CommittedEntryId, req.CommittedEntryMd5) {
			okC = s.pipeline.QueueFlush()
		} else {
			okC = p.Queue(&walle_pb.PutEntryInternalRequest{
				CommittedEntryId:  req.CommittedEntryId,
				CommittedEntryMd5: req.CommittedEntryMd5,
			})
		}
	}
	if req.Entry.EntryId > 0 {
		okC = p.Queue(req)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ok := <-okC:
		if !ok {
			return nil, status.Errorf(codes.OutOfRange, "put entryId: %d, commitId: %d", req.Entry.EntryId, req.CommittedEntryId)
		}
		return &walle_pb.PutEntryInternalResponse{}, nil
	}
}

func (s *Server) fetchCommittedEntry(
	ctx context.Context,
	streamURI string,
	committedEntryId int64,
	committedEntryMd5 []byte) (*walleapi.Entry, error) {
	ss, ok := s.s.Stream(streamURI, true)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "streamURI: %s not found locally", streamURI)
	}
	ssTopology := ss.Topology()
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var errs []error
	errsN := 0
	errsC := make(chan error, len(ssTopology.ServerIds))
	entriesC := make(chan *walleapi.Entry, len(ssTopology.ServerIds))
	for _, serverId := range ssTopology.ServerIds {
		if serverId == s.s.ServerId() {
			continue
		}
		c, err := s.c.ForServer(serverId)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		errsN += 1
		go func(c walle_pb.WalleClient, serverId string) {
			r, err := c.ReadEntries(fetchCtx, &walle_pb.ReadEntriesRequest{
				ServerId:      serverId,
				StreamUri:     ss.StreamURI(),
				StreamVersion: ssTopology.Version,
				StartEntryId:  committedEntryId,
				EndEntryId:    committedEntryId + 1,
			})
			if err != nil {
				errsC <- err
				return
			}
			entry, err := r.Recv()
			if err != nil {
				errsC <- err
				return
			}
			entriesC <- entry
			return
		}(c, serverId)
	}
	for i := 0; i < errsN; i++ {
		select {
		case err := <-errsC:
			errs = append(errs, err)
		case entry := <-entriesC:
			return entry, nil
		}
	}
	return nil, errors.Errorf("%s", errs)
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
		if !ok || entry.EntryId != entryId {
			return status.Errorf(codes.NotFound,
				"entry: %d is missing, found: %d in [%d..%d)",
				entryId, entry.GetEntryId(), req.StartEntryId, req.EndEntryId)
		}
		entryId += 1
		// TODO(zviad): Can `send` block? in that case we might have to treat cursor
		// differently.
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

func (s *Server) processRequestHeader(req requestHeader) (ss storage.Stream, err error) {
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

func (s *Server) checkStreamVersion(ss storage.Metadata, reqStreamVersion int64) error {
	version := ss.Topology().Version
	if reqStreamVersion == version-1 || reqStreamVersion == version || reqStreamVersion == version+1 {
		return nil
	}
	return errors.Errorf("stream[%s] incompatible version: %d vs %d", ss.StreamURI(), reqStreamVersion, version)
}

// Checks writerId if it is still active for a given streamURI. If newer writerId is supplied, will try
// to get updated information from other servers because this server must have missed the NewWriter call.
func (s *Server) checkAndUpdateWriterId(
	ctx context.Context,
	ss storage.Metadata,
	writerId storage.WriterId) error {
	for {
		ssWriterId, writerAddr, _, _ := ss.WriterInfo()
		if writerId == ssWriterId {
			return nil
		}
		if writerId < ssWriterId {
			return status.Errorf(
				codes.FailedPrecondition, "writer no longer active: %s - %s < %s", writerAddr, writerId, ssWriterId)
		}
		resp, err := s.broadcastWriterInfo(ctx, ss)
		if err != nil {
			return err
		}
		respWriterId := storage.WriterId(resp.WriterId)
		if respWriterId <= ssWriterId {
			return status.Errorf(codes.Internal, "writerId is newer than majority?: %s > %s", writerId, ssWriterId)
		}
		zlog.Infof(
			"[%s] writerId: updating %s -> %s",
			ss.StreamURI(), hex.EncodeToString([]byte(ssWriterId)), hex.EncodeToString([]byte(writerId)))
		ss.UpdateWriter(respWriterId, resp.WriterAddr, time.Duration(resp.LeaseMs)*time.Millisecond)
	}
}
