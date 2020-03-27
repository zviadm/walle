package topomgr

import (
	"context"
	"flag"
	"sync"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
	"github.com/zviadm/zlog"
)

var flagManagerLease = flag.Duration(
	"walle.topomgr_lease", wallelib.LeaseMinimum,
	"Lease duration for internal topology manager. Default should be fine in most circumstances, "+
		"unless root cluster is deployed across really high latency network.")

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
	go func() {
		defer close(notifyDone)
		defer zlog.Infof("[tm] stopping management: %s", topologyURI)
		for {
			w, e, err := wallelib.WaitAndClaim(ctx, m.c, topologyURI, m.addr, *flagManagerLease)
			if err != nil {
				return
			}
			topology, err := wallelib.TopologyFromEntry(e)
			if err != nil || topology.Version != e.EntryId {
				// This must never happen!
				// TODO(zviad): Decide on best path forward here. We don't have to crash,
				// theoretically it can be recovered if topology is still valid.
				zlog.Fatalf("[tm] unrecoverable err %s:%d - %s - %s", topologyURI, e.EntryId, topology, err)
			}
			// initialize GoLang structs/maps to avoid `nil` pointer errors.
			if topology.Streams == nil {
				topology.Streams = make(map[string]*walleapi.StreamTopology)
			}
			if topology.Servers == nil {
				topology.Servers = make(map[string]*walleapi.ServerInfo)
			}
			zlog.Infof("[tm] claimed writer: %s, version: %d", topologyURI, topology.Version)
			m.mx.Lock()
			m.perTopo[topologyURI].writer = w
			m.perTopo[topologyURI].topology = topology
			m.mx.Unlock()
			for {
				state, notify := w.WriterState()
				if state == wallelib.Closed {
					zlog.Warningf("[tm] claim lost unexpectedly: %s", topologyURI)
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-notify:
				}
			}
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
	if w := m.perTopo[topologyURI].writer; w != nil {
		w.Close(true)
	}
	delete(m.perTopo, topologyURI)
}
