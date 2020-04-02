package topomgr

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var flagManagerLease = flag.Duration(
	"walle.topomgr_lease", time.Second,
	"Lease duration for internal topology manager. Default should be fine in most circumstances, "+
		"unless root cluster is deployed across really high latency network.")

const (
	Prefix = "/cluster/"
)

type Manager struct {
	c    wallelib.Client
	addr string

	mx       sync.Mutex
	clusters map[string]*clusterData
}

func NewManager(c wallelib.Client, addr string) *Manager {
	return &Manager{
		c:        c,
		addr:     addr,
		clusters: make(map[string]*clusterData),
	}
}

// Manage, StopManaging & Close calls aren't thread-safe, must be called from a single thread only.
func (m *Manager) Close() {
	for clusterURI, _ := range m.clusters {
		m.StopManaging(clusterURI)
	}
}

// Manage, StopManaging & Close calls aren't thread-safe, must be called from a single thread only.
func (m *Manager) Manage(clusterURI string) {
	if _, ok := m.clusters[clusterURI]; ok {
		return
	}
	m.mx.Lock()
	defer m.mx.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	notifyDone := make(chan struct{})
	m.clusters[clusterURI] = &clusterData{
		cancel:     cancel,
		notifyDone: notifyDone,
	}
	go m.manageLoop(ctx, notifyDone, clusterURI)
}

func (m *Manager) manageLoop(
	ctx context.Context,
	notifyDone chan struct{},
	clusterURI string) {
	defer close(notifyDone)
	for {
		w, err := wallelib.WaitAndClaim(ctx, m.c, clusterURI, m.addr, *flagManagerLease)
		if err != nil {
			return // context has expired.
		}
		e := w.Committed()
		topology, err := wallelib.TopologyFromEntry(e)
		if err != nil || topology.Version != e.EntryId {
			// This must never happen!
			// TODO(zviad): Decide on best path forward here. We don't have to crash,
			// theoretically it can be recovered if topology is still valid.
			zlog.Fatalf("[tm] unrecoverable err %s:%d - %s - %s", clusterURI, e.EntryId, topology, err)
		}
		// initialize GoLang structs/maps to avoid `nil` pointer errors.
		if topology.Streams == nil {
			topology.Streams = make(map[string]*walleapi.StreamTopology)
		}
		if topology.Servers == nil {
			topology.Servers = make(map[string]*walleapi.ServerInfo)
		}
		zlog.Infof("[tm] claimed writer: %s, version: %d", clusterURI, topology.Version)
		m.mx.Lock()
		m.clusters[clusterURI].writer = w
		m.clusters[clusterURI].topology = topology
		m.mx.Unlock()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.Done():
				zlog.Warningf("[tm] claim lost unexpectedly: %s", clusterURI)
			}
			break
		}
	}
}

// manage & stopManaging calls aren't thread-safe, must be called from a single thread only.
func (m *Manager) StopManaging(clusterURI string) {
	perTopo, ok := m.clusters[clusterURI]
	if !ok {
		return
	}
	perTopo.cancel()
	<-perTopo.notifyDone

	m.mx.Lock()
	defer m.mx.Unlock()
	if w := m.clusters[clusterURI].writer; w != nil {
		w.Close()
	}
	delete(m.clusters, clusterURI)
}

func (m *Manager) clusterMX(clusterURI string) (c *clusterData, unlock func(), err error) {
	m.mx.Lock()
	defer func() {
		if err != nil {
			m.mx.Unlock()
		}
	}()
	c, ok := m.clusters[clusterURI]
	if !ok || c.writer == nil {
		return nil, nil, status.Errorf(codes.Unavailable, "not serving: %s", clusterURI)
	}
	if !c.writer.IsExclusive() {
		return nil, nil, status.Errorf(codes.Unavailable,
			"writer no longer exclusive: %s", clusterURI)
	}
	return c, m.mx.Unlock, err
}

type clusterData struct {
	cancel     context.CancelFunc
	notifyDone <-chan struct{}
	writer     *wallelib.Writer
	topology   *walleapi.Topology
	putCtx     *wallelib.PutCtx
}

func (c *clusterData) commitTopology() *wallelib.PutCtx {
	topologyB, err := c.topology.Marshal()
	if err != nil {
		panic(err) // this must never happen, crashing is the only sane solution.
	}
	putCtx := c.writer.PutEntry(topologyB)
	c.putCtx = putCtx
	return putCtx
}

func resolvePutCtx(ctx context.Context, putCtx *wallelib.PutCtx, err error) error {
	if err != nil {
		return err
	}
	if putCtx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-putCtx.Done():
		return putCtx.Err()
	}
}
