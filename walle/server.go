package walle

import (
	"context"

	"github.com/zviadm/walle/walle/broadcast"
	"github.com/zviadm/walle/walle/pipeline"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements both WalleApiServer and WalleServer interfaces.
type Server struct {
	rootCtx context.Context
	s       storage.Storage
	c       Client

	pipeline *pipeline.Pipeline
}

// Client wraps both Api client and Direct client interfaces.
type Client interface {
	wallelib.Client
	broadcast.Client
}

// NewServer creates new Server object.
func NewServer(
	ctx context.Context,
	s storage.Storage,
	c Client,
	d wallelib.Discovery,
	topoMgr *topomgr.Manager) *Server {
	r := &Server{
		rootCtx: ctx,
		s:       s,
	}
	r.c = wrapClient(c, s.ServerId(), r)
	r.pipeline = pipeline.New(ctx, s.FlushSync, r.fetchCommittedEntry)

	r.watchTopology(ctx, d, topoMgr)
	go r.writerInfoWatcher(ctx)
	go r.gapHandler(ctx)

	// Renew all writer leases at startup.
	for _, streamURI := range s.Streams(true) {
		ss, ok := s.Stream(streamURI)
		if !ok {
			continue // can race with topology watcher.
		}
		writerId, writerAddr, _, _ := ss.WriterInfo()
		if isInternalWriter(writerAddr) {
			continue
		}
		ss.RenewLease(writerId, wallelib.ReconnectDelay)
	}
	return r
}

type requestHeader interface {
	GetServerId() string
	GetStreamUri() string
	GetStreamVersion() int64
	GetFromServerId() string
}

func (s *Server) processRequestHeader(req requestHeader) (ss storage.Stream, err error) {
	if req.GetServerId() != s.s.ServerId() {
		return nil, status.Errorf(codes.NotFound, "server_id: %s not found", req.GetServerId())
	}
	if req.GetFromServerId() == "" {
		return nil, status.Errorf(codes.NotFound, "from_server_id: %s not found", req.GetFromServerId())
	}
	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%s not found", req.GetStreamUri())
	}
	if err := s.checkStreamVersion(
		ss, req.GetStreamVersion(), req.GetFromServerId()); err != nil {
		return nil, err
	}
	return ss, nil
}

func (s *Server) checkStreamVersion(
	ss storage.Metadata, reqStreamVersion int64, fromServerId string) error {
	ssTopology := ss.Topology()
	if reqStreamVersion == ssTopology.Version ||
		reqStreamVersion == ssTopology.Version+1 ||
		(reqStreamVersion == ssTopology.Version-1 && storage.IsMember(ssTopology, fromServerId)) {
		return nil
	}
	return status.Errorf(
		codes.NotFound, "%s incompatible version: %d vs %d (from: %s)",
		ss.StreamURI(), reqStreamVersion, ssTopology.Version, fromServerId)
}
