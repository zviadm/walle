package walle

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) ClaimWriter(
	ctx context.Context,
	req *walleapi.ClaimWriterRequest) (*walleapi.ClaimWriterResponse, error) {
	if req.LeaseMs != 0 && req.LeaseMs < wallelib.LeaseMinimum.Nanoseconds()/time.Millisecond.Nanoseconds() {
		return nil, status.Errorf(codes.FailedPrecondition, "lease_ms: %d must be >%s", req.LeaseMs, wallelib.LeaseMinimum)
	}
	if req.WriterAddr == "" {
		return nil, status.Error(codes.FailedPrecondition, "writer_addr must be set")
	}

	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "streamURI: %s not found locally", req.GetStreamUri())
	}
	writerId := storage.MakeWriterId()
	ssTopology := ss.Topology()
	serverIds, err := s.broadcastRequest(ctx, ssTopology.ServerIds,
		func(c walle_pb.WalleClient, ctx context.Context, serverId string) error {
			_, err := c.NewWriter(ctx, &walle_pb.NewWriterRequest{
				ServerId:      serverId,
				StreamUri:     req.StreamUri,
				StreamVersion: ssTopology.Version,
				FromServerId:  s.s.ServerId(),
				WriterId:      writerId.Encode(),
				WriterAddr:    req.WriterAddr,
				LeaseMs:       req.LeaseMs,
			})
			return err
		})
	if err != nil {
		return nil, err
	}

	// Following code only operates on serverIds that successfully completed NewWriter call. This makes
	// it safe to do operations serially, since those servers are supposed to be alive and responsive.
	var entries map[string][]*walleapi.Entry
	for {
		entries = make(map[string][]*walleapi.Entry, len(serverIds))
		for _, serverId := range serverIds {
			c, err := s.c.ForServer(serverId)
			if err != nil {
				return nil, err
			}
			r, err := c.LastEntries(ctx, &walle_pb.LastEntriesRequest{
				ServerId:      serverId,
				StreamUri:     req.StreamUri,
				StreamVersion: ssTopology.Version,
				FromServerId:  s.s.ServerId(),
			})
			if err != nil {
				return nil, err
			}
			entries[serverId] = r.Entries
		}
		committed, err := s.commitMaxEntry(
			ctx, req.StreamUri, ssTopology.Version, entries, writerId)
		if err != nil {
			return nil, err
		}
		if !committed {
			break // Nothing to commit, thus all servers are at the same committed entry.
		}
	}

	var maxWriterServerId string
	var maxEntry *walleapi.Entry
	for serverId, es := range entries {
		if len(es) <= 1 {
			continue
		}
		e := es[len(es)-1]
		if maxEntry == nil || e.WriterId > maxEntry.WriterId ||
			(e.WriterId == maxEntry.WriterId && e.EntryId > maxEntry.EntryId) {
			maxWriterServerId = serverId
			maxEntry = e
		}
	}
	if maxEntry == nil {
		// Entries are all fully committed. There is no need to reconcile anything,
		// claiming writer can just succeed.
		for _, es := range entries {
			return &walleapi.ClaimWriterResponse{
				WriterId:  writerId.Encode(),
				LastEntry: es[0],
			}, nil
		}
	}

	maxEntry.WriterId = writerId.Encode()
	c, err := s.c.ForServer(maxWriterServerId)
	if err != nil {
		return nil, err
	}
	_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
		ServerId:      maxWriterServerId,
		StreamUri:     req.StreamUri,
		StreamVersion: ssTopology.Version,
		FromServerId:  s.s.ServerId(),
		Entry:         maxEntry,
	})
	if err != nil {
		return nil, err
	}
	maxEntries := entries[maxWriterServerId]
	for serverId, es := range entries {
		if serverId == maxWriterServerId {
			continue
		}
		c, err := s.c.ForServer(serverId)
		if err != nil {
			return nil, err
		}
		startIdx := len(es)
		for idx, entry := range es {
			if bytes.Compare(entry.ChecksumMd5, maxEntries[idx].ChecksumMd5) != 0 {
				startIdx = idx
				break
			}
		}
		for idx := startIdx; idx < len(maxEntries); idx++ {
			entry := maxEntries[idx]
			entry.WriterId = writerId.Encode()
			_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
				ServerId:      serverId,
				StreamUri:     req.StreamUri,
				StreamVersion: ssTopology.Version,
				FromServerId:  s.s.ServerId(),
				Entry:         entry,
			})
			if err != nil {
				return nil, err
			}
		}
	}
	for serverId, _ := range entries {
		c, err := s.c.ForServer(serverId)
		if err != nil {
			return nil, err
		}
		_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
			ServerId:          serverId,
			StreamUri:         req.StreamUri,
			StreamVersion:     ssTopology.Version,
			FromServerId:      s.s.ServerId(),
			Entry:             maxEntry,
			CommittedEntryId:  maxEntry.EntryId,
			CommittedEntryMd5: maxEntry.ChecksumMd5,
		})
		if err != nil {
			return nil, err
		}
	}
	return &walleapi.ClaimWriterResponse{WriterId: writerId.Encode(), LastEntry: maxEntry}, nil
}

func (s *Server) commitMaxEntry(
	ctx context.Context,
	streamURI string,
	streamVersion int64,
	entries map[string][]*walleapi.Entry,
	writerId storage.WriterId) (bool, error) {
	var maxEntry *walleapi.Entry
	committed := false
	for _, es := range entries {
		entryId := es[0].EntryId
		if maxEntry == nil || entryId > maxEntry.EntryId {
			maxEntry = es[0]
		}
	}
	maxEntry.WriterId = writerId.Encode()
	for serverId, es := range entries {
		if es[0].EntryId < maxEntry.EntryId {
			committed = true
			c, err := s.c.ForServer(serverId)
			if err != nil {
				return false, err
			}
			_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
				ServerId:          serverId,
				StreamUri:         streamURI,
				StreamVersion:     streamVersion,
				FromServerId:      s.s.ServerId(),
				Entry:             maxEntry,
				CommittedEntryId:  maxEntry.EntryId,
				CommittedEntryMd5: maxEntry.ChecksumMd5,
			})
			if err != nil {
				return false, err
			}
		}
	}
	return committed, nil
}

func (s *Server) WriterStatus(
	ctx context.Context,
	req *walleapi.WriterStatusRequest) (*walleapi.WriterStatusResponse, error) {
	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "streamURI: %s not found locally", req.GetStreamUri())
	}
	writerInfo, err := s.broadcastWriterInfo(ctx, ss)
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

func (s *Server) broadcastWriterInfo(
	ctx context.Context, ss storage.Metadata) (*walle_pb.WriterInfoResponse, error) {
	ssTopology := ss.Topology()
	respMx := sync.Mutex{}
	var respMax *walle_pb.WriterInfoResponse
	var remainingMs []int64
	var streamVersions []int64
	_, err := s.broadcastRequest(ctx, ssTopology.ServerIds,
		func(c walle_pb.WalleClient, ctx context.Context, serverId string) error {
			resp, err := c.WriterInfo(ctx, &walle_pb.WriterInfoRequest{
				ServerId:      serverId,
				StreamUri:     ss.StreamURI(),
				StreamVersion: ssTopology.Version,
				FromServerId:  s.s.ServerId(),
			})
			respMx.Lock()
			defer respMx.Unlock()
			if resp.GetWriterId() > respMax.GetWriterId() {
				respMax = resp
			}
			remainingMs = append(remainingMs, resp.GetRemainingLeaseMs())
			streamVersions = append(streamVersions, resp.GetStreamVersion())
			return err
		})
	if err != nil {
		return nil, err
	}
	respMx.Lock()
	defer respMx.Unlock()
	// Sort responses by (writerId, remainingLeaseMs) and choose one that majority is
	// greather than or equal to.
	sort.Slice(remainingMs, func(i, j int) bool { return remainingMs[i] < remainingMs[j] })
	sort.Slice(streamVersions, func(i, j int) bool { return streamVersions[i] < streamVersions[j] })
	respMax.RemainingLeaseMs = remainingMs[len(ssTopology.ServerIds)/2]
	respMax.StreamVersion = streamVersions[len(ssTopology.ServerIds)/2]
	return respMax, nil
}

func (s *Server) PutEntry(
	ctx context.Context, req *walleapi.PutEntryRequest) (*walleapi.PutEntryResponse, error) {
	if req.Entry.GetWriterId() == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "writer_id must be set")
	}
	if len(req.Entry.Data) > wallelib.MaxEntrySize {
		return nil, status.Errorf(
			codes.FailedPrecondition, "entry too large: %d > %d", len(req.Entry.Data), wallelib.MaxEntrySize)
	}

	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "streamURI: %s not found locally", req.GetStreamUri())
	}
	ssTopology := ss.Topology()
	_, err := s.broadcastRequest(ctx, ssTopology.ServerIds,
		func(c walle_pb.WalleClient, ctx context.Context, serverId string) error {
			_, err := c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
				ServerId:          serverId,
				StreamUri:         req.StreamUri,
				StreamVersion:     ssTopology.Version,
				FromServerId:      s.s.ServerId(),
				Entry:             req.Entry,
				CommittedEntryId:  req.CommittedEntryId,
				CommittedEntryMd5: req.CommittedEntryMd5,
			})
			return err
		})
	if err != nil {
		return nil, err
	}
	return &walleapi.PutEntryResponse{}, nil
}

func (s *Server) StreamEntries(
	req *walleapi.StreamEntriesRequest,
	stream walleapi.WalleApi_StreamEntriesServer) error {
	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return status.Errorf(codes.NotFound, "streamURI: %s not found locally", req.GetStreamUri())
	}
	entryId := req.FromEntryId
	internalCtx := stream.Context()
	reqDeadline, ok := stream.Context().Deadline()
	if ok {
		var cancel context.CancelFunc
		internalCtx, cancel = context.WithTimeout(
			internalCtx, reqDeadline.Sub(time.Now())*4/5)
		defer cancel()
	}
	var committedId int64
	for {
		var notify <-chan struct{}
		committedId, notify = ss.CommittedEntryId()
		if entryId < 0 {
			entryId = committedId
		}
		if entryId <= committedId {
			break
		}
		_, writerAddr, _, remainingLease := ss.WriterInfo()
		if remainingLease < 0 && !strings.HasPrefix(writerAddr, writerInternalAddrPrefix) {
			return status.Errorf(codes.Unavailable,
				"writer: %s lease expired for streamURI: %s", writerAddr, req.StreamUri)
		}
		select {
		case <-s.rootCtx.Done():
			return nil
		case <-internalCtx.Done():
			return nil
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-notify:
		}
	}

	oneOk := false
	sendEntry := func(e *walleapi.Entry) error {
		if err := stream.Send(e); err != nil {
			return err
		}
		oneOk = true
		return nil
	}
	err := s.readAndProcessEntries(
		internalCtx, ss, entryId, committedId+1, sendEntry)
	if !oneOk && err != nil {
		return err
	}
	return nil
}

// Broadcasts requests to all serverIds and returns list of serverIds that have succeeded.
// Returns an error if majority didn't succeed.
func (s *Server) broadcastRequest(
	ctx context.Context,
	serverIds []string,
	call func(
		c walle_pb.WalleClient,
		ctx context.Context,
		serverId string) error) (successIds []string, err error) {
	callCtx := ctx
	deadline, ok := ctx.Deadline()
	if ok {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithDeadline(context.Background(), deadline)
		defer func() {
			if err != nil {
				cancel()
			}
		}()
	}
	type callErr struct {
		ServerId string
		Err      error
	}
	errsC := make(chan *callErr, len(serverIds))
	for _, serverId := range serverIds {
		// TODO(zviad): if serverId == s.serverId ==> c should be self wrapped server.
		c, err := s.c.ForServer(serverId)
		if err != nil {
			errsC <- &callErr{ServerId: serverId, Err: err}
			continue
		}
		go func(c walle_pb.WalleClient, serverId string) {
			err := call(c, callCtx, serverId)
			errsC <- &callErr{ServerId: serverId, Err: err}
		}(c, serverId)
	}
	var errCodeFinal codes.Code
	var errs []error
	for i := 0; i < len(serverIds); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-errsC:
			if err.Err != nil {
				errs = append(errs, err.Err)
				errCode := status.Convert(err.Err).Code()
				if errCode == codes.FailedPrecondition || errCodeFinal != codes.FailedPrecondition {
					errCodeFinal = errCode
				}
			} else {
				successIds = append(successIds, err.ServerId)
			}
		}
		if len(successIds) >= len(serverIds)/2+1 {
			return successIds, nil
		}
		if len(errs) >= (len(serverIds)+1)/2 {
			return nil, status.Errorf(errCodeFinal, "errs: %d / %d - %s", len(errs), len(serverIds), errs)
		}
	}
	panic("unreachable code")
}
