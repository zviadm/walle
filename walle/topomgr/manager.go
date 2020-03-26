package topomgr

import (
	"context"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Manager struct {
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

func NewManager(c wallelib.BasicClient, addr string) *Manager {
	return &Manager{
		c:       c,
		addr:    addr,
		perTopo: make(map[string]*perTopoData),
	}
}

// Manage, StopManaging & Close calls aren't thread-safe, must be called from a single thread only.
func (m *Manager) Close() {
	for topologyURI, _ := range m.perTopo {
		m.StopManaging(topologyURI)
	}
}

// Manage, StopManaging & Close calls aren't thread-safe, must be called from a single thread only.
func (m *Manager) Manage(topologyURI string) {
	if _, ok := m.perTopo[topologyURI]; ok {
		return
	}
	m.mx.Lock()
	defer m.mx.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	notifyDone := make(chan struct{})
	m.perTopo[topologyURI] = &perTopoData{
		cancel:     cancel,
		notifyDone: notifyDone,
	}
	lease := wallelib.LeaseMinimum // TODO(zviad): should be configurable.
	go func() {
		defer close(notifyDone)
		defer glog.Infof("[tm:%s] stopping management: %s", topologyURI, m.addr)
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
			glog.Infof("[tm:%s] claimed writer: %s, version: %d", topologyURI, m.addr, topology.Version)
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
			glog.Warningf("[tm:%s] claim lost unexpectedly: %s", topologyURI, m.addr)
		}
	}()
}

// manage & stopManaging calls aren't thread-safe, must be called from a single thread only.
func (m *Manager) StopManaging(topologyURI string) {
	perTopo, ok := m.perTopo[topologyURI]
	if !ok {
		return
	}
	perTopo.cancel()
	<-perTopo.notifyDone

	m.mx.Lock()
	defer m.mx.Unlock()
	m.perTopo[topologyURI] = nil
}

func (m *Manager) perTopoMX(topologyURI string) (p *perTopoData, unlock func(), err error) {
	m.mx.Lock()
	defer func() {
		if err != nil {
			m.mx.Unlock()
		}
	}()
	p, ok := m.perTopo[topologyURI]
	if !ok || p.writer == nil {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "not serving: %s", topologyURI)
	}
	writerState, _ := p.writer.WriterState()
	if writerState != wallelib.Exclusive {
		return nil, nil, status.Errorf(codes.FailedPrecondition,
			"writer no longer exclusive: %s - %s", topologyURI, writerState)
	}
	return p, m.mx.Unlock, err
}

func (p *perTopoData) updateTopology() <-chan error {
	topologyB, err := p.topology.Marshal()
	if err != nil {
		panic(err) // this must never happen, crashing is the only sane solution.
	}
	_, errC := p.writer.PutEntry(topologyB)
	return errC
}

func (m *Manager) RegisterServer(
	ctx context.Context,
	req *topomgr.RegisterServerRequest) (*empty.Empty, error) {
	updateErr, err := m.registerServer(req)
	if err := resolveUpdateErr(ctx, updateErr, err); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (m *Manager) registerServer(req *topomgr.RegisterServerRequest) (<-chan error, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer unlock()

	if proto.Equal(p.topology.Servers[req.ServerId], req.ServerInfo) {
		errC := make(chan error, 1)
		errC <- nil
		return errC, nil
	}
	p.topology.Version += 1
	if p.topology.Servers == nil {
		p.topology.Servers = make(map[string]*walleapi.ServerInfo, 1)
	}
	p.topology.Servers[req.ServerId] = req.ServerInfo
	return p.updateTopology(), nil
}

func (m *Manager) FetchTopology(
	ctx context.Context,
	req *topomgr.FetchTopologyRequest) (*walleapi.Topology, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer unlock()
	entry := p.writer.Committed()
	topology, err := wallelib.TopologyFromEntry(entry)
	if err != nil {
		return nil, err
	}
	return topology, nil
}

func (m *Manager) UpdateTopology(
	ctx context.Context,
	req *topomgr.UpdateTopologyRequest) (*empty.Empty, error) {
	updateErr, err := m.updateTopology(ctx, req)
	if err := resolveUpdateErr(ctx, updateErr, err); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (m *Manager) updateTopology(
	ctx context.Context,
	req *topomgr.UpdateTopologyRequest) (<-chan error, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if p.topology.GetVersion()+1 != req.Topology.Version {
		return nil, status.Errorf(codes.FailedPrecondition,
			"topology version mismatch: %s - %d + 1 != %d",
			req.TopologyUri, p.topology.GetVersion(), req.Topology.Version)
	}
	p.topology = req.Topology
	return p.updateTopology(), nil
}

func resolveUpdateErr(ctx context.Context, updateErr <-chan error, err error) error {
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-updateErr:
		return err
	}
}
