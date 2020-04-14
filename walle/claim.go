package walle

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/broadcast"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ClaimWriter implements WalleApiServer interface.
func (s *Server) ClaimWriter(
	ctx context.Context,
	req *walleapi.ClaimWriterRequest) (*walleapi.ClaimWriterResponse, error) {
	if req.LeaseMs != 0 && req.LeaseMs < wallelib.LeaseMinimum.Nanoseconds()/time.Millisecond.Nanoseconds() {
		return nil, status.Errorf(codes.InvalidArgument, "lease_ms: %d must be >%s", req.LeaseMs, wallelib.LeaseMinimum)
	}
	if req.WriterAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "writer_addr must be set")
	}

	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%s not found", req.GetStreamUri())
	}
	writerId := storage.MakeWriterId()
	ssTopology := ss.Topology()
	serverIds, err := broadcast.Call(ctx, s.c, ssTopology.ServerIds, 0, 0,
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
			r, err := c.TailEntries(ctx, &walle_pb.TailEntriesRequest{
				ServerId:      serverId,
				StreamUri:     req.StreamUri,
				StreamVersion: ssTopology.Version,
				FromServerId:  s.s.ServerId(),
			})
			if err != nil {
				return nil, err
			}
			var rEntries []*walleapi.Entry
			for {
				entry, err := r.Recv()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}
				rEntries = append(rEntries, entry)
			}
			entries[serverId] = rEntries
		}
		committed, err := s.commitMaxEntry(
			ctx, req.StreamUri, ssTopology.Version, entries)
		if err != nil {
			return nil, err
		}
		if !committed {
			break // Nothing to commit, thus all servers are at the same committed entry.
		}
	}

	var maxWriterServerId string
	var maxEntry *walleapi.Entry
	var committedEntryId int64
	var committedEntryXX uint64
	for serverId, es := range entries {
		e := es[len(es)-1]
		cmpWriterId := bytes.Compare(e.WriterId, maxEntry.GetWriterId())
		if maxEntry == nil || cmpWriterId > 0 ||
			(cmpWriterId == 0 && e.EntryId > maxEntry.EntryId) {
			maxWriterServerId = serverId
			maxEntry = e
		}
		committedEntryId = es[0].EntryId
		committedEntryXX = es[0].ChecksumXX
	}

	c, err := s.c.ForServer(maxWriterServerId)
	if err != nil {
		return nil, err
	}
	maxEntry = proto.Clone(maxEntry).(*walleapi.Entry)
	maxEntry.WriterId = writerId.Encode()
	_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
		ServerId:         maxWriterServerId,
		StreamUri:        req.StreamUri,
		StreamVersion:    ssTopology.Version,
		FromServerId:     s.s.ServerId(),
		Entry:            maxEntry,
		CommittedEntryId: committedEntryId,
		CommittedEntryXX: committedEntryXX,
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
		esLen := len(es)
		if len(maxEntries) < esLen {
			esLen = len(maxEntries)
		}
		startIdx := esLen
		for idx, entry := range es[:esLen] {
			if entry.ChecksumXX != maxEntries[idx].ChecksumXX {
				startIdx = idx
				break
			}
		}
		for idx := startIdx; idx < len(maxEntries); idx++ {
			entry := proto.Clone(maxEntries[idx]).(*walleapi.Entry)
			entry.WriterId = writerId.Encode()
			_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
				ServerId:         serverId,
				StreamUri:        req.StreamUri,
				StreamVersion:    ssTopology.Version,
				FromServerId:     s.s.ServerId(),
				Entry:            entry,
				CommittedEntryId: committedEntryId,
				CommittedEntryXX: committedEntryXX,
			})
			if err != nil {
				return nil, err
			}
		}
	}
	for serverId := range entries {
		c, err := s.c.ForServer(serverId)
		if err != nil {
			return nil, err
		}
		_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
			ServerId:         serverId,
			StreamUri:        req.StreamUri,
			StreamVersion:    ssTopology.Version,
			FromServerId:     s.s.ServerId(),
			Entry:            maxEntry,
			CommittedEntryId: maxEntry.EntryId,
			CommittedEntryXX: maxEntry.ChecksumXX,
		})
		if err != nil {
			return nil, err
		}
	}
	return &walleapi.ClaimWriterResponse{WriterId: writerId.Encode(), TailEntry: maxEntry}, nil
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
				ServerId:         serverId,
				StreamUri:        streamURI,
				StreamVersion:    streamVersion,
				FromServerId:     s.s.ServerId(),
				Entry:            maxEntry,
				CommittedEntryId: maxEntry.EntryId,
				CommittedEntryXX: maxEntry.ChecksumXX,
				IgnoreLeaseRenew: true,
			})
			if err != nil {
				return false, err
			}
		}
	}
	return committed, nil
}

// NewWriter implements WalleServer interface.
func (s *Server) NewWriter(
	ctx context.Context,
	req *walle_pb.NewWriterRequest) (*walle_pb.NewWriterResponse, error) {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return nil, err
	}
	reqWriterId := storage.WriterId(req.WriterId)
	zlog.Infof(
		"[%s] writerId update: %s (%s)", ss.StreamURI(), req.WriterAddr, reqWriterId)
	remainingLease, err := ss.UpdateWriter(reqWriterId, req.WriterAddr, time.Duration(req.LeaseMs)*time.Millisecond)
	if err != nil {
		return nil, err
	}
	// Need to wait `remainingLease` duration before returning. However, we also need to make sure new
	// lease doesn't expire since writer client can't heartbeat until this call succeeds.
	if remainingLease > 0 {
		err := ss.RenewLease(reqWriterId, remainingLease)
		if err != nil {
			return nil, err
		}
		time.Sleep(remainingLease)
	}
	return &walle_pb.NewWriterResponse{}, nil
}

// TailEntries implements WalleServer interface.
func (s *Server) TailEntries(
	req *walle_pb.TailEntriesRequest, stream walle_pb.Walle_TailEntriesServer) error {
	ss, err := s.processRequestHeader(req)
	if err != nil {
		return err
	}
	entries, err := ss.TailEntries(0)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := stream.Send(entry); err != nil {
			return err
		}
	}
	return nil
}
