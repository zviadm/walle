package wallelib

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
)

type BasicClient interface {
	ForStream(streamURI string) (walleapi.WalleApiClient, error)
}

type DirectClient interface {
	ForServer(serverId string) (walle_pb.WalleClient, error)
}

type client struct {
	d Discovery

	mx        sync.Mutex
	topology  *walleapi.Topology
	conns     map[string]*grpc.ClientConn
	preferred map[string][]string // streamURI -> []serverId
}

func NewClient(ctx context.Context, d Discovery) *client {
	c := &client{
		d:     d,
		conns: make(map[string]*grpc.ClientConn),
	}
	topology, notify := d.Topology()
	c.update(topology)
	go c.watcher(ctx, notify)
	return c
}

// Helper wrapper to create new client for a specific topology. It is
// important for `rootFile` to exist and contain correct address for at least
// one server that serves rootURI stream.
func NewClientForTopology(
	ctx context.Context,
	rootURI string,
	rootFile string,
	topologyURI string,
	topologyFile string) (*client, error) {
	rootD, err := NewRootDiscovery(ctx, rootURI, rootFile)
	if err != nil {
		return nil, err
	}
	rootCli := NewClient(ctx, rootD)
	if topologyURI == "" {
		return rootCli, nil
	}

	d, err := NewDiscovery(ctx, rootCli, rootURI, topologyURI, topologyFile)
	if err != nil {
		return nil, err
	}
	c := NewClient(ctx, d)
	return c, nil
}

func (c *client) watcher(ctx context.Context, notify <-chan struct{}) {
	var topology *walleapi.Topology
	for {
		select {
		case <-notify:
		case <-ctx.Done():
			return
		}
		topology, notify = c.d.Topology()
		c.update(topology)
	}
}

func (c *client) update(topology *walleapi.Topology) {
	preferred := make(map[string][]string, len(topology.Streams))
	for streamURI, streamT := range topology.Streams {
		preferredIds := make([]string, len(streamT.ServerIds))
		copy(preferredIds, streamT.ServerIds)
		// TODO(zviad): actually sort by preference.
		preferred[streamURI] = preferredIds
	}

	c.mx.Lock()
	defer c.mx.Unlock()
	c.topology = topology
	c.preferred = preferred
	// Close and clear out all connections to serverIds that are no longer registered in topology.
	for serverId, conn := range c.conns {
		_, ok := topology.Servers[serverId]
		if !ok {
			conn.Close()
			delete(c.conns, serverId)
		}
	}
}

func (c *client) ForStream(streamURI string) (walleapi.WalleApiClient, error) {
	c.mx.Lock()
	defer c.mx.Unlock()
	preferredIds, ok := c.preferred[streamURI]
	if !ok {
		return nil, errors.Errorf("streamURI: %s, not found in topology", streamURI)
	}
	preferredMajority := (len(preferredIds) + 1) / 2
	var oneErr error
	for idx := 0; idx < len(preferredIds); idx++ {
		var serverId string
		if idx < preferredMajority {
			serverId = preferredIds[0]
			copy(preferredIds, preferredIds[1:])
			preferredIds[len(preferredIds)-1] = serverId
		} else {
			serverId = preferredIds[idx]
		}

		conn, err := c.unsafeServerConn(serverId)
		if err != nil {
			oneErr = err
			continue
		}
		if conn.GetState() == connectivity.TransientFailure {
			continue
		}
		return walleapi.NewWalleApiClient(conn), nil
	}
	return nil, errors.Errorf(
		"connections to servers for: %s are all in failed state, err: %v", streamURI, oneErr)
}

func (c *client) ForServer(serverId string) (walle_pb.WalleClient, error) {
	c.mx.Lock()
	defer c.mx.Unlock()
	conn, err := c.unsafeServerConn(serverId)
	if err != nil {
		return nil, err
	}
	return walle_pb.NewWalleClient(conn), nil
}

func (c *client) unsafeServerConn(serverId string) (*grpc.ClientConn, error) {
	conn, ok := c.conns[serverId]
	if ok {
		return conn, nil
	}
	serverAddr, ok := c.topology.Servers[serverId]
	if !ok {
		return nil, errors.Errorf("serverId: %s, not found in topology", serverId)
	}
	// TODO(zviad): Decide what to do about security...
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure()) // Non-Blocking Dial.
	if err != nil {
		return nil, err
	}
	return conn, nil
}
