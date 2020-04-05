package walle

import (
	"context"
	"strings"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

const (
	writerTimeoutToResolve   = wallelib.LeaseMinimum
	writerTimeoutToReResolve = 10 * writerTimeoutToResolve
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
			_, writerAddr, _, remainingLease := ss.WriterInfo()
			timeoutToResolve := writerTimeoutToResolve
			if isInternalWriter(writerAddr) {
				timeoutToResolve = writerTimeoutToReResolve
			}
			if remainingLease >= -timeoutToResolve {
				continue // Quick shortcut, requiring no i/o for most common case.
			}
			wInfo, err := s.broadcastWriterInfo(ctx, ss)
			if err != nil {
				zlog.Warningf("[ww] err fetching writerInfo %s: %s", streamURI, err)
				continue
			}
			if time.Duration(wInfo.RemainingLeaseMs)*time.Millisecond >= -timeoutToResolve {
				continue
			}
			writerAddr = writerInternalAddrPrefix + s.s.ServerId()
			if !isInternalWriter(wInfo.WriterAddr) {
				zlog.Infof(
					"[ww] resolving stream %s (prev: %s, %dms) ",
					streamURI, wInfo.WriterAddr, wInfo.RemainingLeaseMs)
			}
			_, err = s.ClaimWriter(ctx,
				&walleapi.ClaimWriterRequest{StreamUri: streamURI, WriterAddr: writerAddr})
			if err != nil && !isInternalWriter(wInfo.WriterAddr) {
				zlog.Warningf(
					"[ww] err resolving %s, (prev: %s, %dms) -- %s",
					streamURI, wInfo.WriterAddr, wInfo.RemainingLeaseMs, err)
				continue
			}
		}
	}
}

func isInternalWriter(writerAddr string) bool {
	return writerAddr == "" || strings.HasPrefix(writerAddr, writerInternalAddrPrefix)
}
