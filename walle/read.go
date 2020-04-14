package walle

import (
	"context"
	"io"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// maxSizeToSend is maximum amount of data that will be sent on a single stream
	// before it is closed.
	maxSizeToSend = 4 * 1024 * 1024
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
		committedId, notify := ss.CommittedEntryId()
		if committedId >= req.PollEntryId {
			break
		}
		_, writerAddr, _, remainingLease := ss.WriterInfo()
		minimumLease := time.Duration(0)
		if isInternalWriter(writerAddr) {
			minimumLease = -reResolveTimeout
		}
		checkForLease := remainingLease - minimumLease
		if checkForLease < 0 {
			return nil, status.Errorf(codes.Unavailable,
				"writer: %s lease expired for streamURI: %s", writerAddr, req.StreamUri)
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
	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return status.Errorf(codes.NotFound, "%s not found", req.GetStreamUri())
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
		minimumLease := time.Duration(0)
		if isInternalWriter(writerAddr) {
			minimumLease = -reResolveTimeout
		}
		if remainingLease < minimumLease {
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

	streamedSize := 0
	oneOk := false
	sendEntry := func(e *walleapi.Entry) error {
		if err := stream.Send(e); err != nil {
			return err
		}
		streamedSize += e.Size()
		oneOk = true
		if streamedSize > maxSizeToSend {
			return io.EOF
		}
		return nil
	}
	err := s.readAndProcessEntries(
		internalCtx, ss, entryId, committedId+1, sendEntry)
	if !oneOk && err != nil {
		return err
	}
	return nil
}

// ReadEntries implements WalleServer interface.
func (s *Server) ReadEntries(
	req *walle_pb.ReadEntriesRequest, stream walle_pb.Walle_ReadEntriesServer) error {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return err
	}
	cursor, err := ss.ReadFrom(req.StartEntryId)
	if err != nil {
		return err
	}
	defer cursor.Close()
	streamedSize := 0
	for entryId := req.StartEntryId; entryId < req.EndEntryId && streamedSize < maxSizeToSend; entryId++ {
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
		streamedSize += entry.Size()
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

// readEntriesAll makes ReadEntries request and consumes all entries from the stream. Keeping streams open
// for too long makes code more complicated, it is better to stream and process things large chunk by chunk.
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
