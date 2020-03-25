package walle

import (
	"context"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
)

type topoManager struct {
	c    wallelib.BasicClient
	addr string

	mx      sync.Mutex
	perTopo map[string]*perTopoData
}

type perTopoData struct {
	cancel     context.CancelFunc
	notifyDone <-chan struct{}
	writer     *wallelib.Writer
	topology   *walleapi.Topology
}

func newTopoManager(c wallelib.BasicClient, addr string) *topoManager {
	return &topoManager{
		c:       c,
		addr:    addr,
		perTopo: make(map[string]*perTopoData),
	}
}

// manage & stopManaging calls aren't thread-safe, must be called from a single thread only.
func (m *topoManager) Manage(ctx context.Context, topologyURI string) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if _, ok := m.perTopo[topologyURI]; ok {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	notifyDone := make(chan struct{})
	m.perTopo[topologyURI] = &perTopoData{
		cancel:     cancel,
		notifyDone: notifyDone,
	}
	lease := time.Second // should be configurable.
	go func() {
		defer close(notifyDone)
		for {
			w, e, err := wallelib.WaitAndClaim(ctx, m.c, topologyURI, m.addr, lease)
			if err != nil {
				return
			}
			topology, err := wallelib.TopologyFromEntry(e)
			if err != nil || topology.Version != e.EntryId {
				// This must never happen!
				glog.Errorf("[tm:%s] unrecoverable err (%d): %s, %s", topologyURI, e.EntryId, topology, err)
				topology = &walleapi.Topology{Version: e.EntryId} // TODO(zviad): Decide on best path forward here.
			}
			m.mx.Lock()
			m.perTopo[topologyURI].writer = w
			m.perTopo[topologyURI].topology = topology
			m.mx.Unlock()
			for {
				state, notify := w.WriterState()
				if state == wallelib.Closed {
					break
				}
				select {
				case <-ctx.Done():
					w.Close(true)
					return
				case <-notify:
				}
			}
		}
	}()
}

// manage & stopManaging calls aren't thread-safe, must be called from a single thread only.
func (m *topoManager) StopManaging(ctx context.Context, topologyURI string) {
	m.mx.Lock()
	defer m.mx.Unlock()
	perTopo, ok := m.perTopo[topologyURI]
	if !ok {
		return
	}
	perTopo.cancel()
	notify := perTopo.notifyDone
	m.mx.Unlock()
	<-notify
	m.mx.Lock()
	m.perTopo[topologyURI] = nil
}

func (s *Server) RegisterServer(
	ctx context.Context,
	req *walle_pb.RegisterServerRequest) (*walle_pb.RegisterServerResponse, error) {
	updateErr, err := s.topoMgr.RegisterServer(req.TopologyStreamUri, req.ServerId, req.ServerAddr)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err = <-updateErr:
		if err != nil {
			return nil, err
		}
		return &walle_pb.RegisterServerResponse{}, nil
	}
}

func (m *topoManager) RegisterServer(
	topologyURI string, serverId string, serverAddr string) (updateErr <-chan error, err error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	perTopo, ok := m.perTopo[topologyURI]
	if !ok || perTopo.writer == nil {
		return nil, errors.Errorf("not serving topologyURI: %s", topologyURI)
	}
	writerState, _ := perTopo.writer.WriterState()
	if writerState != wallelib.Exclusive {
		return nil, errors.Errorf("topologyURI: %s writer in bad state: %s", topologyURI, writerState)
	}
	perTopo.topology.Version += 1
	perTopo.topology.Servers[serverId] = serverAddr
	topologyB, err := perTopo.topology.Marshal()
	panicOnErr(err) // this must never happen, crashing is the only sane solution.
	_, updateErr = perTopo.writer.PutEntry(topologyB)
	return updateErr, nil
}
