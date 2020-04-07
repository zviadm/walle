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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Broadcast for PutEntry calls will wait `putEntryLiveWait` amount of time for all calls to finish
	// within request itself. And will continue waiting upto `putEntryBgWait` amount of time in the background
	// before canceling context. This is useful to make sure live healthy servers aren't falling too far behind.
	putEntryLiveWait = 10 * time.Millisecond
	putEntryBgWait   = time.Second
)

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
	var committedEntryMd5 []byte
	for serverId, es := range entries {
		e := es[len(es)-1]
		cmpWriterId := bytes.Compare(e.WriterId, maxEntry.GetWriterId())
		if maxEntry == nil || cmpWriterId > 0 ||
			(cmpWriterId == 0 && e.EntryId > maxEntry.EntryId) {
			maxWriterServerId = serverId
			maxEntry = e
		}
		committedEntryId = es[0].EntryId
		committedEntryMd5 = es[0].ChecksumMd5
	}

	c, err := s.c.ForServer(maxWriterServerId)
	if err != nil {
		return nil, err
	}
	maxEntry = proto.Clone(maxEntry).(*walleapi.Entry)
	maxEntry.WriterId = writerId.Encode()
	_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
		ServerId:          maxWriterServerId,
		StreamUri:         req.StreamUri,
		StreamVersion:     ssTopology.Version,
		FromServerId:      s.s.ServerId(),
		Entry:             maxEntry,
		CommittedEntryId:  committedEntryId,
		CommittedEntryMd5: committedEntryMd5,
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
			if bytes.Compare(entry.ChecksumMd5, maxEntries[idx].ChecksumMd5) != 0 {
				startIdx = idx
				break
			}
		}
		for idx := startIdx; idx < len(maxEntries); idx++ {
			entry := proto.Clone(maxEntries[idx]).(*walleapi.Entry)
			entry.WriterId = writerId.Encode()
			_, err = c.PutEntryInternal(ctx, &walle_pb.PutEntryInternalRequest{
				ServerId:          serverId,
				StreamUri:         req.StreamUri,
				StreamVersion:     ssTopology.Version,
				FromServerId:      s.s.ServerId(),
				Entry:             entry,
				CommittedEntryId:  committedEntryId,
				CommittedEntryMd5: committedEntryMd5,
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
				ServerId:          serverId,
				StreamUri:         streamURI,
				StreamVersion:     streamVersion,
				FromServerId:      s.s.ServerId(),
				Entry:             maxEntry,
				CommittedEntryId:  maxEntry.EntryId,
				CommittedEntryMd5: maxEntry.ChecksumMd5,
				IgnoreLeaseRenew:  true,
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
		return nil, status.Errorf(codes.NotFound, "%s not found", req.GetStreamUri())
	}

	writerInfo, err := broadcast.WriterInfo(
		ctx, s.c, s.s.ServerId(), ss.StreamURI(), ss.Topology())
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

func (s *Server) PutEntry(
	ctx context.Context, req *walleapi.PutEntryRequest) (*walleapi.PutEntryResponse, error) {
	if len(req.Entry.GetWriterId()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "writer_id must be set")
	}
	if len(req.Entry.Data) > wallelib.MaxEntrySize {
		return nil, status.Errorf(
			codes.InvalidArgument, "entry too large: %d > %d", len(req.Entry.Data), wallelib.MaxEntrySize)
	}

	ss, ok := s.s.Stream(req.GetStreamUri())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%s not found", req.GetStreamUri())
	}
	ssTopology := ss.Topology()
	_, err := broadcast.Call(ctx, s.c, ssTopology.ServerIds, putEntryLiveWait, putEntryBgWait,
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
			minimumLease = -2 * writerTimeoutToReResolve
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
