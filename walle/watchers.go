package walle

import (
	"context"
	"strings"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/broadcast"
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
	for streamURI, streamT := range t.Streams {
		err := s.s.Update(streamURI, streamT)
		if err != nil {
			zlog.Errorf("ERR_FATAL; err updating topology: %s %s", streamURI, err)
			continue
		}
		if topoMgr == nil || !strings.HasPrefix(streamURI, topomgr.Prefix) {
			continue
		}
		_, ok := s.s.Stream(streamURI)
		if ok {
			topoMgr.Manage(streamURI)
		} else {
			topoMgr.StopManaging(streamURI)
		}
	}
}

// Watches all streams to find if there are any streams that don't have an active
// writer. If there are no active writers for a stream, it might need resolving of
// committed entries.
func (s *Server) writerInfoWatcher(ctx context.Context) {
	ticker := time.NewTicker(writerTimeoutToResolve)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		streamURIs := s.s.Streams(true)
		for _, streamURI := range streamURIs {
			ss, ok := s.s.Stream(streamURI)
			if !ok {
				continue // can race with topology watcher.
			}
			s.checkAndResolveNoWriterStream(ctx, ss)
		}
	}
}

func (s *Server) checkAndResolveNoWriterStream(ctx context.Context, ss storage.Stream) {
	_, writerAddr, _, remainingLease := ss.WriterInfo()
	timeoutToResolve := writerTimeoutToResolve
	if isInternalWriter(writerAddr) {
		if strings.HasSuffix(writerAddr, s.s.ServerId()) {
			timeoutToResolve = reResolveFrequency
		} else {
			timeoutToResolve = reResolveTimeout
		}
	}
	if remainingLease >= -timeoutToResolve {
		return // Quick shortcut, requiring no i/o for most common case.
	}
	ctx, cancel := context.WithTimeout(ctx, reResolveTimeout)
	defer cancel()
	wInfo, err := broadcast.WriterInfo(
		ctx, s.c, s.s.ServerId(), ss.StreamURI(), ss.Topology())
	if err != nil {
		return // TODO(zviad): Should we log a warning?
	}
	if time.Duration(wInfo.RemainingLeaseMs)*time.Millisecond >= -timeoutToResolve {
		return
	}
	err = s.resolveNoWriterStream(ctx, ss, timeoutToResolve)
	if err != nil && status.Convert(err).Code() != codes.FailedPrecondition {
		zlog.Warningf(
			"[ww] err resolving %s, (prev: %s, %dms) -- %s",
			ss.StreamURI(), wInfo.WriterAddr, wInfo.RemainingLeaseMs, err)
	}
}

func (s *Server) resolveNoWriterStream(
	ctx context.Context, ss storage.Stream, timeoutToResolve time.Duration) error {
	writerAddr := writerInternalAddrPrefix + s.s.ServerId()
	resp, err := s.ClaimWriter(ctx,
		&walleapi.ClaimWriterRequest{StreamUri: ss.StreamURI(), WriterAddr: writerAddr})
	if err != nil {
		return err
	}
	_, err = s.PutEntry(ctx, &walleapi.PutEntryRequest{
		StreamUri:         ss.StreamURI(),
		Entry:             resp.TailEntry,
		CommittedEntryId:  resp.TailEntry.EntryId,
		CommittedEntryMd5: resp.TailEntry.ChecksumMd5,
	})
	return err
}

func isInternalWriter(writerAddr string) bool {
	return writerAddr == "" || strings.HasPrefix(writerAddr, writerInternalAddrPrefix)
}
