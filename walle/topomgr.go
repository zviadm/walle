package walle

import (
	"context"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/topomgr"
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
			glog.Warning("[tm:%s] claim lost")
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

func (m *topoManager) RegisterServer(
	ctx context.Context,
	req *topomgr.RegisterServerRequest) (*empty.Empty, error) {
	updateErr, err := m.registerServer(req)
	if err := resolveUpdateErr(ctx, updateErr, err); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (m *topoManager) registerServer(req *topomgr.RegisterServerRequest) (<-chan error, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	perTopo, ok := m.perTopo[req.TopologyUri]
	if !ok || perTopo.writer == nil {
		return nil, errors.Errorf("not serving topologyURI: %s", req.TopologyUri)
	}
	writerState, _ := perTopo.writer.WriterState()
	if writerState != wallelib.Exclusive {
		return nil, errors.Errorf("topologyURI: %s writer no longer exclusive: %s", req.TopologyUri, writerState)
	}
	if proto.Equal(perTopo.topology.Servers[req.ServerId], req.ServerInfo) {
		errC := make(chan error, 1)
		errC <- nil
		return errC, nil
	}
	perTopo.topology.Version += 1
	perTopo.topology.Servers[req.ServerId] = req.ServerInfo
	topologyB, err := perTopo.topology.Marshal()
	panicOnErr(err) // this must never happen, crashing is the only sane solution.
	_, errC := perTopo.writer.PutEntry(topologyB)
	return errC, nil
}

func (m *topoManager) FetchTopology(
	ctx context.Context,
	req *topomgr.FetchTopologyRequest) (*walleapi.Topology, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	perTopo, ok := m.perTopo[req.TopologyUri]
	if !ok || perTopo.writer == nil {
		return nil, errors.Errorf("not serving topologyURI: %s", req.TopologyUri)
	}
	entry := perTopo.writer.Committed()
	writerState, _ := perTopo.writer.WriterState()
	if writerState != wallelib.Exclusive {
		return nil, errors.Errorf("topologyURI: %s writer no longer exclusive: %s", req.TopologyUri, writerState)
	}

	topology, err := wallelib.TopologyFromEntry(entry)
	if err != nil {
		return nil, err
	}
	return topology, nil
}

func (m *topoManager) UpdateTopology(
	ctx context.Context,
	req *topomgr.UpdateTopologyRequest) (*empty.Empty, error) {
	updateErr, err := m.updateTopology(ctx, req)
	if err := resolveUpdateErr(ctx, updateErr, err); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (m *topoManager) updateTopology(
	ctx context.Context,
	req *topomgr.UpdateTopologyRequest) (<-chan error, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	perTopo, ok := m.perTopo[req.TopologyUri]
	if !ok || perTopo.writer == nil {
		return nil, errors.Errorf("not serving topologyURI: %s", req.TopologyUri)
	}
	writerState, _ := perTopo.writer.WriterState()
	if writerState != wallelib.Exclusive {
		return nil, errors.Errorf("topologyURI: %s writer no longer exclusive: %s", req.TopologyUri, writerState)
	}
	if perTopo.topology.GetVersion()+1 != req.Topology.Version {
		return nil, errors.Errorf(
			"topologyURI: %s topology version mismatch: %d + 1 != %d",
			req.TopologyUri, perTopo.topology.GetVersion(), req.Topology.Version)
	}
	perTopo.topology = req.Topology
	topologyB, err := perTopo.topology.Marshal()
	if err != nil {
		return nil, err
	}
	_, updateErr := perTopo.writer.PutEntry(topologyB)
	return updateErr, err
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
