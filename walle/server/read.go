package server

import (
	"context"
	"io"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PollStream implements WalleApiServer interface.
func (s *Server) PollStream(
	ctx context.Context,
	req *walleapi.PollStreamRequest) (*walleapi.Entry, error) {
	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%s not found", req.GetStreamUri())
	}
	reqDeadline, ok := ctx.Deadline()
	if ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(
			ctx, reqDeadline.Sub(time.Now())*4/5)
		defer cancel()
	}
	for {
		checkForLease, err := s.checkForLease(ss)
		if err != nil {
			return nil, err
		}
		notify := ss.CommitNotify()
		committedId := ss.CommittedId()
		if committedId >= req.PollEntryId {
			break
		}
		select {
		case <-s.rootCtx.Done():
			return nil, s.rootCtx.Err()
		case <-ctx.Done():
			return nil, status.Errorf(codes.OutOfRange,
				"committed id: %d < %d", committedId, req.PollEntryId)
		case <-notify:
		case <-time.After(checkForLease):
		}
	}
	entries, err := ss.TailEntries(1)
	if err != nil {
		return nil, err
	}
	return entries[0], nil
}

// StreamEntries implements WalleApiServer interface.
func (s *Server) StreamEntries(
	req *walleapi.StreamEntriesRequest,
	stream walleapi.WalleApi_StreamEntriesServer) error {
	if req.StartEntryId >= req.EndEntryId {
		return status.Errorf(codes.InvalidArgument, "invalid range: [%d..%d)", req.StartEntryId, req.EndEntryId)
	}
	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return status.Errorf(codes.NotFound, "%s not found", req.GetStreamUri())
	}
	var committed int64
	startId := req.StartEntryId
	for {
		notify := ss.CommitNotify()
		committed = ss.CommittedId()
		if committed >= startId {
			break
		}
		checkForLease, err := s.checkForLease(ss)
		if err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-notify:
		case <-time.After(checkForLease):
		}
	}
	for {
		endId := committed + 1
		if endId > req.EndEntryId {
			endId = req.EndEntryId
		}
		err := s.readAndProcessEntries(
			stream.Context(), ss, startId, endId, stream.Send, false)
		if err != nil {
			return err
		}
		if endId == req.EndEntryId {
			return nil
		}
		startId = endId
		for {
			notify := ss.CommitNotify()
			committed = ss.CommittedId()
			if committed >= endId {
				break
			}
			checkForLease, err := s.checkForLease(ss)
			if err != nil {
				return err
			}
			select {
			case <-stream.Context().Done():
				return stream.Context().Err()
			case <-notify:
			case <-time.After(checkForLease):
			}
		}
	}
}

// checkForLease checks if writer for a given stream is still active and heartbeating.
// If heartbeats aren't coming in, server will start rejecting reads to make sure clients
// aren't blocked for too long.
func (s *Server) checkForLease(ss storage.Stream) (time.Duration, error) {
	_, writerAddr, _, remainingLease := ss.WriterInfo()
	if writerAddr == "" {
		return 0, status.Errorf(codes.Unavailable, "%s: not initialized", ss.StreamURI())
	}
	minimumLease := -writerTimeoutToResolve
	if isInternalWriter(writerAddr) {
		minimumLease = -reResolveTimeout
	}
	checkForLease := remainingLease - minimumLease
	if checkForLease < 0 {
		return 0, status.Errorf(
			codes.Unavailable, "%s: writer: %s lease expired", ss.StreamURI(), writerAddr)
	}
	return checkForLease, nil
}

// ReadEntries implements WalleServer interface.
func (s *Server) ReadEntries(
	req *walle_pb.ReadEntriesRequest, stream walle_pb.Walle_ReadEntriesServer) error {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return err
	}
	defer s.inflightReqs.Add(-1)
	cursor, err := ss.ReadFrom(req.StartEntryId)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for entryId := req.StartEntryId; entryId < req.EndEntryId; entryId++ {
		eId, ok := cursor.Next()
		if !ok || eId != entryId {
			return status.Errorf(codes.NotFound,
				"entry: %d is missing, found: %d in [%d..%d)",
				entryId, eId, req.StartEntryId, req.EndEntryId)
		}
		entry := cursor.Entry()
		err := stream.Send(entry)
		if err != nil {
			return err
		}
	}
	return nil
}

// fetchCommittedEntry fetches single missing committed entry from other servers. This can potentially
// block hot path of putting new entries, thus it should be optimized for speed.
func (s *Server) fetchCommittedEntry(
	ctx context.Context,
	streamURI string,
	committedEntryId int64,
	committedEntryXX uint64) (*walleapi.Entry, error) {
	ss, ok := s.s.Stream(streamURI)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%s not found", streamURI)
	}
	ssTopology := ss.Topology()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	serverIds := make([]string, 0, len(ssTopology.ServerIds)-1)
	for _, serverId := range ssTopology.ServerIds {
		if serverId == s.s.ServerId() {
			continue
		}
		serverIds = append(serverIds, serverId)
	}
	if len(serverIds) == 0 {
		return nil, status.Errorf(codes.NotFound, "no other servers available to fetch entry from")
	}
	entriesC := make(chan *walleapi.Entry, len(serverIds))
	errC := make(chan error, len(serverIds))
	for _, serverId := range serverIds {
		c, err := s.c.ForServer(serverId)
		if err != nil {
			errC <- err
			continue
		}
		go func(c walle_pb.WalleClient, serverId string) {
			entries, err := readEntriesAll(ctx, c, &walle_pb.ReadEntriesRequest{
				ServerId:      serverId,
				StreamUri:     ss.StreamURI(),
				StreamVersion: ssTopology.Version,
				FromServerId:  s.s.ServerId(),
				StartEntryId:  committedEntryId,
				EndEntryId:    committedEntryId + 1,
			})
			if len(entries) < 1 {
				errC <- err
				return
			}
			entriesC <- entries[0]
		}(c, serverId)
	}
	var errs []error
	for range serverIds {
		select {
		case entry := <-entriesC:
			return entry, nil
		case err := <-errC:
			errs = append(errs, err)
		}
	}
	return nil, status.Errorf(codes.Unavailable, "errs: %d / %d - %s", len(errs), len(serverIds), errs)
}

// readEntriesAll makes ReadEntries request and consumes all entries from the stream.
func readEntriesAll(
	ctx context.Context,
	c walle_pb.WalleClient,
	req *walle_pb.ReadEntriesRequest) ([]*walleapi.Entry, error) {
	var entries []*walleapi.Entry
	r, err := c.ReadEntries(ctx, req)
	if err != nil {
		return entries, err
	}
	for {
		entry, err := r.Recv()
		if err != nil {
			if err == io.EOF {
				return entries, nil
			}
			return entries, err
		}
		entries = append(entries, entry)
	}
}
