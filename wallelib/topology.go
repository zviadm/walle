package wallelib

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type discovery struct {
	root       Client
	clusterURI string

	mx       sync.Mutex
	topology *walleapi.Topology
	notify   chan struct{}
}

// Discovery represents topology discovery for a single WALLE cluster.
type Discovery interface {
	Topology() (*walleapi.Topology, <-chan struct{})
}

// StaticDiscovery implements Discovery interface, always returning same
// topology.
type StaticDiscovery struct {
	T *walleapi.Topology
}

// Topology implements Discovery interface.
func (d *StaticDiscovery) Topology() (*walleapi.Topology, <-chan struct{}) {
	return d.T, nil
}

// NewRootDiscovery creates discovery for root cluster. If waitForRefresh is true,
// will wait for a bit to try to fetch most up to date topology information from
// root cluster.
func NewRootDiscovery(
	ctx context.Context,
	rootPb *walleapi.Topology,
	waitForRefresh bool) (Discovery, error) {
	if rootPb.GetVersion() == 0 {
		return nil, errors.Errorf("must provide valid root topology: %+v", rootPb)
	}
	d := newDiscovery(nil, rootPb.RootUri, rootPb)
	d.root = NewClient(ctx, d)
	_, notify := d.Topology()
	go d.watcher(ctx)
	if waitForRefresh {
		initCtx, cancel := context.WithTimeout(ctx, 2*watchTimeout)
		defer cancel()
		select {
		case <-notify:
		case <-initCtx.Done():
			zlog.Warningf("refreshing topology for: %s timedout", rootPb.RootUri)
		}
	}
	return d, nil
}

// NewDiscovery creates Discovery for a non root cluster. Supplied `topology` can be nil,
// in that case if initial fetch fails, this function will return an error.
func NewDiscovery(
	ctx context.Context,
	root Client,
	clusterURI string,
	topology *walleapi.Topology) (Discovery, error) {
	d := newDiscovery(root, clusterURI, topology)
	_, notify := d.Topology()
	go d.watcher(ctx)
	initCtx, cancel := context.WithTimeout(ctx, 2*watchTimeout)
	defer cancel()
	select {
	case <-notify:
	case <-initCtx.Done():
		if topology == nil {
			return nil, status.Errorf(codes.Unavailable, "err initializing topology: %s", clusterURI)
		}
		zlog.Warningf("refreshing topology for: %s timedout", clusterURI)
	}
	return d, nil
}

func newDiscovery(
	root Client,
	clusterURI string,
	topology *walleapi.Topology) *discovery {
	return &discovery{
		root:       root,
		clusterURI: clusterURI,

		topology: topology,
		notify:   make(chan struct{}),
	}
}

func (d *discovery) watcher(ctx context.Context) {
	var version int64 = -1
	for {
		err := KeepTryingWithBackoff(
			ctx, connectTimeout, watchTimeout,
			func(retryN uint) (bool, bool, error) {
				cli, err := d.root.ForStream(d.clusterURI)
				if err != nil {
					return false, false, err
				}
				topology, err := streamUpdates(ctx, cli, d.clusterURI, version)
				if err != nil {
					if err == io.EOF {
						return true, false, nil
					}
					return false, false, err
				}
				d.updateTopology(topology)
				version = topology.Version + 1
				return true, false, nil
			})
		if err != nil {
			return
		}
	}
}

func (d *discovery) updateTopology(topology *walleapi.Topology) {
	d.mx.Lock()
	defer d.mx.Unlock()
	if d.topology == nil || topology.Version > d.topology.Version {
		d.topology = topology
	}
	close(d.notify)
	d.notify = make(chan struct{})
}

func (d *discovery) Topology() (*walleapi.Topology, <-chan struct{}) {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.topology, d.notify
}

func streamUpdates(
	ctx context.Context,
	cli walleapi.WalleApiClient,
	clusterURI string,
	fromEntryId int64) (*walleapi.Topology, error) {
	streamCtx, cancel := context.WithTimeout(ctx, watchTimeout)
	defer cancel()
	r, err := cli.StreamEntries(streamCtx, &walleapi.StreamEntriesRequest{
		StreamUri:   clusterURI,
		FromEntryId: fromEntryId,
	})
	if err != nil {
		return nil, err
	}
	var entry *walleapi.Entry
	for {
		entryNew, err := r.Recv()
		if err != nil {
			if entry == nil {
				return nil, err
			}
			break
		}
		entry = entryNew
	}
	return TopologyFromEntry(entry)
}

// TopologyFromFile reads and parses topology from a file.
func TopologyFromFile(f string) (*walleapi.Topology, error) {
	topologyB, err := ioutil.ReadFile(f)
	if err != nil {
		return nil, err
	}
	topology := &walleapi.Topology{}
	err = topology.Unmarshal(topologyB)
	return topology, err
}

// TopologyToFile writes topology to a file. Write happens atomically
// to avoid chances of corruption if process where to crash.
func TopologyToFile(t *walleapi.Topology, f string) error {
	tB, err := t.Marshal()
	if err != nil {
		return err
	}
	tmpF := f + ".tmp"
	if err := ioutil.WriteFile(tmpF, tB, 0644); err != nil {
		return err
	}
	// Write file atomically, to avoid any corruption issues if program
	// crashes in the middle of a write.
	return os.Rename(tmpF, f)
}

// TopologyFromEntry parses out and unmarshalls stored topology protobuf from an entry.
func TopologyFromEntry(entry *walleapi.Entry) (*walleapi.Topology, error) {
	topology := &walleapi.Topology{}
	if err := topology.Unmarshal(entry.Data); err != nil {
		return nil, err
	}
	if topology.Version != entry.EntryId {
		return nil, errors.Errorf(
			"invalid topology entry, version must match EntryId: %d vs %s", entry.EntryId, topology)
	}
	return topology, nil
}
