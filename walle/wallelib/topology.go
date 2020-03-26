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
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type discovery struct {
	root        BasicClient
	topologyURI string

	mx       sync.Mutex
	topology *walleapi.Topology
	notify   chan struct{}
}

type Discovery interface {
	Topology() (*walleapi.Topology, <-chan struct{})
}

func NewRootDiscovery(
	ctx context.Context,
	rootURI string,
	rootTopology *walleapi.Topology) (Discovery, error) {
	if rootTopology.GetVersion() == 0 {
		return nil, errors.Errorf("must provide valid root topology: %+v", rootTopology)
	}
	d := newDiscovery(nil, rootURI, rootTopology)
	d.root = NewClient(ctx, d)
	go d.watcher(ctx)
	return d, nil
}

func NewDiscovery(
	ctx context.Context,
	root BasicClient,
	topologyURI string,
	topology *walleapi.Topology) (Discovery, error) {
	if topology.GetVersion() == 0 {
		retryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		err := KeepTryingWithBackoff(
			retryCtx, time.Second/10, time.Second,
			func(retryN uint) (bool, error) {
				cli, err := root.ForStream(topologyURI)
				if err != nil {
					return false, err
				}
				topology, err = streamUpdates(retryCtx, cli, topologyURI, -1)
				return true, err
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
	root BasicClient,
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
		cli, err := d.root.ForStream(d.topologyURI)
		if err != nil {
			zlog.Warningf("[%s] watcher can't connect: %s...", d.topologyURI, err)
			// TODO(zviad): exp backoff?
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		topology, err = streamUpdates(ctx, cli, d.topologyURI, d.topology.Version+1)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			errStatus, _ := status.FromError(err)
			if err == io.EOF ||
				err == context.DeadlineExceeded ||
				errStatus.Code() == codes.DeadlineExceeded {
				continue
			}
			zlog.Warningf("[%s] watcher err: %s", d.topologyURI, err)
			// TODO(zviad): exp backoff?
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		d.updateTopology(topology)
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
		if err == nil {
			entry = entryNew
			continue
		}
		if entry == nil {
			return nil, err
		}
		break
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
