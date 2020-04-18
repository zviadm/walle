package server

import (
	"context"
	"strings"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/broadcast"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// If there is no active writer for a stream, once writerTimeoutToResolve amount of time
	// passes after lease is fully expired, one of the stream member nodes will try to claim the
	// writer to make sure committed entries are resolved in the stream.
	// Node that resolves the stream will continue to re-resolve it at reResolveFrequency. However
	// if that node goes down too for some reason, some other node will start re-resolving after
	// reResolveTimeout.
	writerTimeoutToResolve = wallelib.LeaseMinimum
	reResolveFrequency     = time.Second
	reResolveTimeout       = 10 * time.Second

	writerInternalAddrPrefix = "_internal:"
)

func (s *Server) watchTopology(ctx context.Context, d wallelib.Discovery, topoMgr *topomgr.Manager) {
	topology, notify := d.Topology()
	s.updateTopology(topology, topoMgr)
	go func() {
		defer s.s.Close()
		if topoMgr != nil {
			defer topoMgr.Close()
		}
		for {
			select {
			case <-notify:
			case <-ctx.Done():
				return
			}
			topology, notify = d.Topology()
			zlog.Infof("[tw] received version: %d", topology.Version)
			s.updateTopology(topology, topoMgr)
		}
	}()
}
func (s *Server) updateTopology(t *walleapi.Topology, topoMgr *topomgr.Manager) {
	// First apply topologies for non-local streams. This makes sure streams get removed
	// first before new streams get added.
	for streamURI, streamT := range t.Streams {
		if storage.IsMember(streamT, s.s.ServerId()) {
			continue
		}
		err := s.s.CrUpdateStream(streamURI, streamT)
		panic.OnErr(err)
		if topoMgr != nil && strings.HasPrefix(streamURI, topomgr.Prefix) {
			topoMgr.StopManaging(streamURI)
		}
	}
	for streamURI, streamT := range t.Streams {
		if !storage.IsMember(streamT, s.s.ServerId()) {
			continue
		}
		err := s.s.CrUpdateStream(streamURI, streamT)
		if err != nil {
			zlog.Errorf("ERR_FATAL; err updating topology: %s %s", streamURI, err)
			continue
		}
		if topoMgr != nil && strings.HasPrefix(streamURI, topomgr.Prefix) {
			topoMgr.Manage(streamURI)
		}
	}
}

// Watches all streams to find if there are any streams that don't have an active
// writer. If there are no active writers for a stream, it might need resolving of
// committed entries.
func (s *Server) writerInfoWatcher(ctx context.Context) {
	ticker := time.NewTicker(writerTimeoutToResolve)
	defer ticker.Stop()
	putEntryReqs := make(map[string]*walleapi.PutEntryRequest) // streamURI -> ...
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		streamURIs := s.s.LocalStreams()
		for _, streamURI := range streamURIs {
			ss, ok := s.s.Stream(streamURI)
			if !ok {
				continue // can race with topology watcher.
			}
			putEntryReqs[streamURI] = s.checkAndResolveNoWriterStream(ctx, ss, putEntryReqs[streamURI])
		}
	}
}

func (s *Server) checkAndResolveNoWriterStream(
	ctx context.Context,
	ss storage.Stream,
	putEntryReq *walleapi.PutEntryRequest) *walleapi.PutEntryRequest {
	selfAddr := writerInternalAddrPrefix + s.s.ServerId()
	_, writerAddr, _, remainingLease := ss.WriterInfo()
	if remainingLease >= -timeoutToResolve(selfAddr, writerAddr) {
		return putEntryReq // Quick shortcut, requiring no i/o for most common case.
	}
	ctx, cancel := context.WithTimeout(ctx, reResolveTimeout)
	defer cancel()
	wInfo, err := broadcast.WriterInfo(
		ctx, s.c, s.s.ServerId(), ss.StreamURI(), ss.Topology())
	if err != nil {
		return putEntryReq // TODO(zviad): Should we log a warning?
	}
	if time.Duration(wInfo.RemainingLeaseMs)*time.Millisecond >=
		-timeoutToResolve(selfAddr, wInfo.WriterAddr) {
		return putEntryReq
	}
	if selfAddr != wInfo.WriterAddr {
		putEntryReq = nil
	}
	putEntryReq, err = s.resolveNoWriterStream(ctx, ss, selfAddr, putEntryReq)
	if err != nil && status.Convert(err).Code() != codes.FailedPrecondition {
		zlog.Warningf(
			"[ww] err resolving %s, (prev: %s, %dms) -- %s",
			ss.StreamURI(), wInfo.WriterAddr, wInfo.RemainingLeaseMs, err)
	}
	return putEntryReq
}

func timeoutToResolve(selfAddr string, writerAddr string) time.Duration {
	if writerAddr == selfAddr {
		return reResolveFrequency
	}
	if isInternalWriter(writerAddr) {
		return reResolveTimeout
	}
	return writerTimeoutToResolve
}

func (s *Server) resolveNoWriterStream(
	ctx context.Context,
	ss storage.Stream,
	writerAddr string,
	putEntryReq *walleapi.PutEntryRequest) (*walleapi.PutEntryRequest, error) {
	if putEntryReq == nil {
		resp, err := s.ClaimWriter(ctx,
			&walleapi.ClaimWriterRequest{StreamUri: ss.StreamURI(), WriterAddr: writerAddr})
		if err != nil {
			return nil, err
		}
		putEntryReq = &walleapi.PutEntryRequest{
			StreamUri:        ss.StreamURI(),
			Entry:            &walleapi.Entry{WriterId: resp.TailEntry.WriterId},
			CommittedEntryId: resp.TailEntry.EntryId,
			CommittedEntryXX: resp.TailEntry.ChecksumXX,
		}
	}
	_, err := s.PutEntry(ctx, putEntryReq)
	if err != nil {
		return nil, err
	}
	return putEntryReq, nil
}

func isInternalWriter(writerAddr string) bool {
	return writerAddr == "" || strings.HasPrefix(writerAddr, writerInternalAddrPrefix)
}
