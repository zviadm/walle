package walle

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"time"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) ClaimWriter(
	ctx context.Context,
	req *walleapi.ClaimWriterRequest) (*walleapi.ClaimWriterResponse, error) {
	ss, ok := s.s.Stream(req.GetStreamUri(), true)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "streamURI: %s not found locally", req.GetStreamUri())
	}
	writerId := makeWriterId()
	ssTopology := ss.Topology()
	serverIds, err := s.broadcastRequest(ctx, ssTopology.ServerIds,
		func(c walle_pb.WalleClient, serverId string) error {
			_, err := c.NewWriter(ctx, &walle_pb.NewWriterRequest{
				ServerId:      serverId,
				StreamUri:     req.StreamUri,
				StreamVersion: ssTopology.Version,
				WriterId:      writerId,
			})
			return err
		})
	if err != nil {
		return nil, err
	}

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
			})
			if err != nil {
				return nil, err
			}
			entries[serverId] = r.Entries
		}
		committed, err := s.commitMaxEntry(ctx, req.StreamUri, ssTopology.Version, entries)
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
		e := es[len(es)-1]
		if maxEntry == nil || e.WriterId > maxEntry.WriterId ||
			(e.WriterId == maxEntry.WriterId && e.EntryId > maxEntry.EntryId) {
			maxWriterServerId = serverId
			maxEntry = e
		}
	}
	maxEntry.WriterId = writerId
	c, err := s.c.ForServer(maxWriterServerId)
	if err != nil {
		return nil, err
	}
	_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
		ServerId:      maxWriterServerId,
		StreamUri:     req.StreamUri,
		StreamVersion: ssTopology.Version,
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
			entry.WriterId = writerId
			_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
				ServerId:      serverId,
				StreamUri:     req.StreamUri,
				StreamVersion: ssTopology.Version,
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
			Entry:             maxEntry,
			CommittedEntryId:  maxEntry.EntryId,
			CommittedEntryMd5: maxEntry.ChecksumMd5,
		})
		if err != nil {
			return nil, err
		}
	}
	return &walleapi.ClaimWriterResponse{WriterId: writerId, LastEntry: maxEntry}, nil
}

func (s *Server) commitMaxEntry(
	ctx context.Context,
	streamURI string,
	streamVersion int64,
	entries map[string][]*walleapi.Entry) (bool, error) {
	var maxEntry *walleapi.Entry
	committed := false
	for _, es := range entries {
		entryId := es[0].EntryId
		if maxEntry == nil || entryId > maxEntry.EntryId {
			maxEntry = es[0]
		}
	}
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

func (s *Server) PutEntry(
	ctx context.Context, req *walleapi.PutEntryRequest) (*walleapi.PutEntryResponse, error) {

	ss, ok := s.s.Stream(req.GetStreamUri(), true)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "streamURI: %s not found locally", req.GetStreamUri())
	}
	ssTopology := ss.Topology()
	_, err := s.broadcastRequest(ctx, ssTopology.ServerIds,
		func(c walle_pb.WalleClient, serverId string) error {
			_, err := c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
				ServerId:          serverId,
				StreamUri:         req.StreamUri,
				StreamVersion:     ssTopology.Version,
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
	ss, ok := s.s.Stream(req.GetStreamUri(), true)
	if !ok {
		return status.Errorf(codes.NotFound, "streamURI: %s not found locally", req.GetStreamUri())
	}
	entryId := req.FromEntryId
	for {
		_, committedId, notify := ss.CommittedEntryIds()
		if entryId < 0 {
			entryId = committedId
		}
		if entryId > committedId {
			select {
			case <-notify:
			case <-stream.Context().Done():
				// TODO(zviad): return nil, only if writer heartbeat is alive.
				return nil
			}
			continue
		}
		cursor := ss.ReadFrom(entryId)
		entry, ok := cursor.Next()
		if !ok {
			return status.Errorf(codes.Internal, "committed entry missing? %d <= %d", entryId, committedId)
		}
		if entry.EntryId > entryId {
			if err := s.fetchAndStoreEntries(
				stream.Context(), ss, entryId, entry.EntryId, stream.Send); err != nil {
				return err
			}
		}
		entryId = entry.EntryId + 1
		err := stream.Send(entry)
		if err != nil {
			return err
		}
	}
}

func makeWriterId() string {
	writerId := make([]byte, 16)
	binary.BigEndian.PutUint64(writerId[0:8], uint64(time.Now().UnixNano()))
	rand.Read(writerId[8:15])
	return string(writerId)
}

func (s *Server) broadcastRequest(
	ctx context.Context,
	serverIds []string,
	call func(c walle_pb.WalleClient, serverId string) error) ([]string, error) {
	var successIds []string
	var errs []error
	// TODO(zviad): needs to be done in parallel.
	for _, serverId := range serverIds {
		var c walle_pb.WalleClient
		var err error
		// if serverId == s.serverId ==> c should be self wrapped server.
		c, err = s.c.ForServer(serverId)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		err = call(c, serverId)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		successIds = append(successIds, serverId)
	}
	if len(successIds) <= len(errs) {
		return nil, errors.Errorf("not enough success: %s <= %d\nerrs: %v", successIds, len(errs), errs)
	}
	return successIds, nil
}
