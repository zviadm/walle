package wallelib

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
)

type discovery struct {
	root        Client
	topologyURI string

	mx       sync.Mutex
	topology *walleapi.Topology
	notify   chan struct{}
}

type Discovery interface {
	Topology() (*walleapi.Topology, <-chan struct{})
}

type StaticDiscovery struct {
	T *walleapi.Topology
}

func (d *StaticDiscovery) Topology() (*walleapi.Topology, <-chan struct{}) {
	return d.T, nil
}

func NewRootDiscovery(
	ctx context.Context,
	rootPb *walleapi.Topology) (Discovery, error) {
	if rootPb.GetVersion() == 0 {
		return nil, errors.Errorf("must provide valid root topology: %+v", rootPb)
	}
	d := newDiscovery(nil, rootPb.RootUri, rootPb)
	d.root = NewClient(ctx, d)
	go d.watcher(ctx)
	return d, nil
}

func NewDiscovery(
	ctx context.Context,
	root Client,
	topologyURI string,
	topology *walleapi.Topology) (Discovery, error) {
	if topology.GetVersion() == 0 {
		retryCtx, cancel := context.WithTimeout(ctx, 5*time.Second) // TODO(zviad): Configure timeout?
		defer cancel()
		err := KeepTryingWithBackoff(
			retryCtx, time.Second, time.Second,
			func(retryN uint) (bool, bool, error) {
				cli, err := root.ForStream(topologyURI, -1)
				if err != nil {
					return false, false, err
				}
				topology, err = streamUpdates(retryCtx, cli, topologyURI, -1)
				if err != nil {
					return false, false, err
				}
				return true, false, nil
			})
		if err != nil {
			return nil, err
		}
	}
	d := newDiscovery(root, topologyURI, topology)
	go d.watcher(ctx)
	return d, nil
}

func newDiscovery(
	root Client,
	topologyURI string,
	topology *walleapi.Topology) *discovery {
	return &discovery{
		root:        root,
		topologyURI: topologyURI,

		topology: topology,
		notify:   make(chan struct{}),
	}
}

func (d *discovery) watcher(ctx context.Context) {
	topology := d.topology
	for {
		err := KeepTryingWithBackoff(
			ctx, LeaseMinimum, time.Second,
			func(retryN uint) (bool, bool, error) {
				cli, err := d.root.ForStream(d.topologyURI, -1)
				if err != nil {
					return false, false, err
				}
				topology, err = streamUpdates(ctx, cli, d.topologyURI, d.topology.Version+1)
				if err != nil {
					if err == io.EOF {
						return true, false, nil
					}
					return false, false, err
				}
				d.updateTopology(topology)
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
	d.topology = topology
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
	topologyURI string,
	fromEntryId int64) (*walleapi.Topology, error) {
	// TODO(zviad): timeout must come from config.
	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	r, err := cli.StreamEntries(streamCtx, &walleapi.StreamEntriesRequest{
		StreamUri:   topologyURI,
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

// Helper function to read topology from a file.
func TopologyFromFile(f string) (*walleapi.Topology, error) {
	topologyB, err := ioutil.ReadFile(f)
	if err != nil {
		return nil, err
	}
	topology := &walleapi.Topology{}
	err = topology.Unmarshal(topologyB)
	return topology, err
}

// Helper function to write topology to a file. Write happens atomically
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

// Parses out and unmarshalls stored topology protobuf from an entry.
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
