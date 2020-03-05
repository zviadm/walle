package wallelib

import (
	"context"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type discovery struct {
	root        BasicClient
	rootURI     string
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
	seedAddrs map[string]string) Discovery {
	seedIds := make([]string, 0, len(seedAddrs))
	for serverId := range seedAddrs {
		seedIds = append(seedIds, serverId)
	}
	topology := &walleapi.Topology{
		Streams: map[string]*walleapi.StreamTopology{
			rootURI: &walleapi.StreamTopology{
				Version:   0,
				ServerIds: seedIds,
			},
		},
		Servers: seedAddrs,
	}
	d := newDiscovery(nil, rootURI, rootURI, topology)
	d.root = NewClient(ctx, d)
	go d.watcher(ctx, 0)
	return d
}

func NewDiscovery(
	ctx context.Context,
	root BasicClient,
	rootURI string,
	topologyURI string) Discovery {
	var topology *walleapi.Topology
	var entryId int64
	cli, err := root.ForStream(rootURI)
	if err == nil {
		topology, entryId, err = streamUpdates(ctx, cli, topologyURI, 0)
	}
	if err != nil {
		glog.Warningf("initializing discovery for: %s, from %s err: %v", topologyURI, rootURI, err)
		topology = &walleapi.Topology{}
		entryId = 0
	}
	d := newDiscovery(root, rootURI, topologyURI, topology)
	go d.watcher(ctx, entryId)
	return d
}

func newDiscovery(
	root BasicClient,
	rootURI string,
	topologyURI string,
	topology *walleapi.Topology) *discovery {
	return &discovery{
		root:        root,
		rootURI:     rootURI,
		topologyURI: rootURI,

		topology: topology,
		notify:   make(chan struct{}),
	}
}

func (d *discovery) watcher(ctx context.Context, entryId int64) {
	var topology *walleapi.Topology
	for {
		cli, err := d.root.ForStream(d.rootURI)
		if err != nil {
			glog.Warningf("[%s] watcher can't connect to root: %s, err: %s", d.topologyURI, d.rootURI, err)
			// TODO(zviad): introduce a delay.
			continue
		}
		topology, entryId, err = streamUpdates(ctx, cli, d.topologyURI, entryId+1)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			errStatus, _ := status.FromError(err)
			if err == context.DeadlineExceeded || errStatus.Code() == codes.DeadlineExceeded {
				continue
			}
			glog.Warningf("[%s] watcher err: %s", d.topologyURI, err)
			// TODO(zviad): Some delay here?
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

func fetchTopologyFromSeeds(
	ctx context.Context,
	seedAddrs []string,
	rootURI string) (*walleapi.Topology, int64, error) {
	for _, addr := range seedAddrs {
		connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		conn, err := grpc.DialContext(connectCtx, addr, grpc.WithBlock())
		if err != nil {
			continue
		}
		cli := walleapi.NewWalleApiClient(conn)
		topology, entryId, err := streamUpdates(ctx, cli, rootURI, -1)
		if err != nil {
			continue
		}
		return topology, entryId, nil
	}
	return nil, 0, errors.Errorf("unable to fetch initial topology")
}

func streamUpdates(
	ctx context.Context,
	cli walleapi.WalleApiClient,
	topologyURI string,
	fromEntryId int64) (*walleapi.Topology, int64, error) {
	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second) // TODO(zviad): timeout must come from config.
	defer cancel()
	r, err := cli.StreamEntries(streamCtx, &walleapi.StreamEntriesRequest{
		StreamUri:   topologyURI,
		FromEntryId: fromEntryId,
	})
	if err != nil {
		return nil, 0, err
	}
	var entry *walleapi.Entry
	for {
		entry, err = r.Recv()
		if err != nil {
			if entry == nil {
				return nil, 0, err
			}

			topology := &walleapi.Topology{}
			err := topology.Unmarshal(entry.Data)
			if err != nil {
				return nil, 0, err
			}
			return topology, entry.EntryId, nil
		}
	}
}
