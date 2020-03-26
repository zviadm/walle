package walle

import (
	"context"
	"encoding/hex"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/walle/wallelib"
)

const (
	writerTimeoutToResolve = time.Second // TODO(zviad): should this be a flag?
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
			glog.Infof("[tw] received version: %d", topology.Version)
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
			glog.Infof("[tw:%s] creating with topology: %+v", streamURI, streamT)
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
			_, _, _, remainingLease := ss.WriterInfo()
			if remainingLease >= -writerTimeoutToResolve {
				continue // Quick shortcut, requiring no i/o for most common case.
			}
			wInfo, err := s.broadcastWriterInfo(ctx, ss)
			if err != nil {
				glog.Warningf("[ww:%s] writer info err: %s", streamURI, err)
				continue
			}
			if time.Duration(wInfo.RemainingLeaseMs)*time.Millisecond >= -writerTimeoutToResolve {
				continue
			}
			writerAddr := "_internal:" + hex.EncodeToString([]byte(s.s.ServerId())) // TODO(zviad): better string
			resp, err := s.ClaimWriter(ctx,
				&walleapi.ClaimWriterRequest{StreamUri: streamURI, WriterAddr: writerAddr})
			if err != nil {
				glog.Warningf("[ww:%s] writer resolve err: %s", streamURI, err)
				continue
			}
			glog.Infof("[ww:%s] resolved stream, %s @entry: %d", streamURI, writerAddr, resp.LastEntry.EntryId)
		}
	}
}
