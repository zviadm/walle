package walle

import (
	"context"
	"encoding/hex"
	"strings"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

const (
	writerTimeoutToResolve   = wallelib.LeaseMinimum // TODO(zviad): should this be a flag?
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
		ss, ok := s.s.Stream(streamURI, false)
		if ok {
			ss.UpdateTopology(streamT)
		} else {
			zlog.Infof("[tw] creating with topology: %s %+s", streamURI, streamT)
			ss = s.s.NewStream(streamURI, streamT)
		}

		if topoMgr == nil || !strings.HasPrefix(streamURI, "/topology/") {
			continue
		}
		if ss.IsLocal() {
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
			ss, ok := s.s.Stream(streamURI, true)
			if !ok {
				continue
			}
			_, writerAddr, _, remainingLease := ss.WriterInfo()
			if writerAddr == "" || remainingLease >= -writerTimeoutToResolve {
				continue // Quick shortcut, requiring no i/o for most common case.
			}
			wInfo, err := s.broadcastWriterInfo(ctx, ss)
			if err != nil {
				zlog.Warningf("[ww] err fetching writerInfo %s: %s", streamURI, err)
				continue
			}
			if time.Duration(wInfo.RemainingLeaseMs)*time.Millisecond >= -writerTimeoutToResolve {
				continue
			}
			writerAddr = writerInternalAddrPrefix + hex.EncodeToString([]byte(s.s.ServerId()))
			resp, err := s.ClaimWriter(ctx,
				&walleapi.ClaimWriterRequest{StreamUri: streamURI, WriterAddr: writerAddr})
			if err != nil {
				zlog.Warningf("[ww] err resolving %s: %s", streamURI, err)
				continue
			}
			zlog.Infof(
				"[ww] resolved stream %s @entry: %d, (prev: %s, %dms) ",
				streamURI, resp.LastEntry.EntryId, wInfo.WriterAddr, wInfo.RemainingLeaseMs)
		}
	}
}
