package wallelib

import (
	"context"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type discovery struct {
	root         BasicClient
	rootURI      string
	topologyURI  string
	topologyFile string

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
	topologyFile string) (Discovery, error) {
	topology, err := topologyFromFile(topologyFile)
	if err != nil {
		return nil, err
	}
	d := newDiscovery(nil, rootURI, rootURI, topologyFile, topology)
	d.root = NewClient(ctx, d)
	go d.watcher(ctx)
	return d, nil
}

func NewDiscovery(
	ctx context.Context,
	root BasicClient,
	rootURI string,
	topologyURI string,
	topologyFile string) (Discovery, error) {
	topology, err := topologyFromFile(topologyFile)
	if err != nil {
		glog.Infof(
			"topology file: %s couldn't be read, will try to stream from scratch, err: %v",
			topologyFile, err)
		cli, err := root.ForStream(rootURI)
		if err != nil {
			return nil, err
		}
		topology, err = streamUpdates(ctx, cli, topologyURI, 0)
		if err != nil {
			return nil, err
		}
		if err := TopologyToFile(topology, topologyFile); err != nil {
			return nil, err
		}
	}
	d := newDiscovery(root, rootURI, topologyURI, topologyFile, topology)
	go d.watcher(ctx)
	return d, nil
}

func topologyFromFile(f string) (*walleapi.Topology, error) {
	topologyB, err := ioutil.ReadFile(f)
	if err != nil {
		return nil, err
	}
	topology := &walleapi.Topology{}
	err = topology.Unmarshal(topologyB)
	return topology, err
}

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

func newDiscovery(
	root BasicClient,
	rootURI string,
	topologyURI string,
	topologyFile string,
	topology *walleapi.Topology) *discovery {
	return &discovery{
		root:         root,
		rootURI:      rootURI,
		topologyURI:  rootURI,
		topologyFile: topologyFile,

		topology: topology,
		notify:   make(chan struct{}),
	}
}

func (d *discovery) watcher(ctx context.Context) {
	topology := d.topology
	for {
		cli, err := d.root.ForStream(d.rootURI)
		if err != nil {
			glog.Warningf("[%s] watcher can't connect to root: %s, err: %s...", d.topologyURI, d.rootURI, err)
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
			if err == context.DeadlineExceeded || errStatus.Code() == codes.DeadlineExceeded {
				continue
			}
			glog.Warningf("[%s] watcher err: %s", d.topologyURI, err)
			// TODO(zviad): exp backoff?
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		d.updateTopology(topology)
		if err := TopologyToFile(topology, d.topologyFile); err != nil {
			glog.Warningf("[%s] watcher failed to store, err: %s", d.topologyURI, err)
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
	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second) // TODO(zviad): timeout must come from config.
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
	topology := &walleapi.Topology{}
	if err := topology.Unmarshal(entry.Data); err != nil {
		return nil, err
	}
	if topology.Version != entry.EntryId {
		return nil, errors.Errorf(
			"invalid topology entry, version must match EntryId: %v vs %+v", entry.EntryId, topology)
	}
	return topology, nil
}
