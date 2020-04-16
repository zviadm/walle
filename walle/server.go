package walle

import (
	"context"
	"flag"
	"sync"

	"github.com/zviadm/walle/walle/broadcast"
	"github.com/zviadm/walle/walle/pipeline"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	flagMaxInternalInflightRequests = flag.Int(
		"walle.max_internal_inflight_requests", 16*1024,
		"Once this limit is reached, all new requests get rejected with RESOURCE_EXHAUSTED errors.")
)

// Server implements both WalleApiServer and WalleServer interfaces.
type Server struct {
	rootCtx context.Context
	s       storage.Storage
	c       Client

	pipeline *pipeline.Pipeline

	maxInflightReqs int64
	inflightReqs    atomic.Int64

	fetchWriterIdMX sync.Mutex

	mxGap          sync.Mutex
	notifyGapC     chan struct{}
	streamsWithGap map[string]struct{}
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
		rootCtx:         ctx,
		s:               s,
		maxInflightReqs: int64(*flagMaxInternalInflightRequests),
		notifyGapC:      make(chan struct{}, 1),
		streamsWithGap:  make(map[string]struct{}),
	}
	r.c = wrapClient(c, s.ServerId(), r)
	r.pipeline = pipeline.New(ctx, r.fetchCommittedEntry, r.notifyGap)

	r.watchTopology(ctx, d, topoMgr)
	go r.writerInfoWatcher(ctx)
	go r.backfillGapsLoop(ctx)

	// Renew all writer leases at startup.
	for _, streamURI := range s.LocalStreams() {
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
	defer func() {
		if err != nil {
			s.inflightReqs.Add(-1)
		}
	}()
	if s.inflightReqs.Add(1) > s.maxInflightReqs {
		return nil, status.Errorf(codes.ResourceExhausted, "too many requests in-flight: %d", s.maxInflightReqs)
	}
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
