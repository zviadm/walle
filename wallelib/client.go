package wallelib

import (
	"context"
	"math/rand"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
)

type Client interface {
	// Returns gRPC client to talk to specific streamURI within a cluster.
	ForStream(streamURI string) (walleapi.WalleApiClient, error)
}

type client struct {
	d Discovery

	mx        sync.Mutex
	topology  *walleapi.Topology
	conns     map[string]*grpc.ClientConn // serverId -> conn
	preferred map[string][]string         // streamURI -> []serverId
	rrIdx     map[string]int              // streamURI -> round-robin index
}

var ErrConnUnavailable = status.Error(codes.Unavailable, "connection in TransientFailure")

func NewClient(ctx context.Context, d Discovery) *client {
	c := &client{
		d:     d,
		conns: make(map[string]*grpc.ClientConn),
		rrIdx: make(map[string]int),
	}
	topology, notify := d.Topology()
	c.update(topology)
	go c.watcher(ctx, notify)
	return c
}

func NewClientFromRootPb(
	ctx context.Context,
	rootPb *walleapi.Topology,
	topologyURI string) (*client, error) {
	rootD, err := NewRootDiscovery(ctx, rootPb)
	if err != nil {
		return nil, err
	}
	rootC := NewClient(ctx, rootD)
	if topologyURI == "" || topologyURI == rootPb.RootUri {
		return rootC, nil
	}
	d, err := NewDiscovery(ctx, rootC, topologyURI, nil)
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
			c.update(&walleapi.Topology{})
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

		// TODO(zviad): actually sort by preference, instead of randomizing the order.
		rand.Shuffle(len(preferredIds), func(i, j int) {
			preferredIds[i], preferredIds[j] = preferredIds[j], preferredIds[i]
		})
		preferred[streamURI] = preferredIds
	}

	c.mx.Lock()
	defer c.mx.Unlock()
	c.topology = topology
	c.preferred = preferred
	// Close and clear out all connections to serverIds that are no longer registered in topology.
	for serverId, conn := range c.conns {
		_, ok := topology.Servers[serverId]
		// TODO(zviad): we should also close connections that may now have incorrect
		// targets, if server address has changed.
		if !ok {
			conn.Close()
			delete(c.conns, serverId)
		}
	}
	for streamURI := range c.rrIdx {
		if _, ok := c.preferred[streamURI]; !ok {
			delete(c.rrIdx, streamURI)
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
	offset := c.rrIdx[streamURI]
	c.rrIdx[streamURI] += 1
	majorityN := len(preferredIds)/2 + 1
	minorityN := len(preferredIds) - majorityN
	for tryN := 0; tryN < 2; tryN++ {
		for i := 0; i < len(preferredIds); i++ {
			var idx int
			if i < majorityN {
				idx = (offset + i) % majorityN
			} else {
				idx = majorityN + (offset+i)%minorityN
			}
			serverId := preferredIds[idx]
			conn, err := c.unsafeServerConn(serverId)
			if err != nil {
				continue
			}
			connState := conn.GetState()
			notReady := connState != connectivity.Ready && connState != connectivity.Idle
			if (tryN == 0 && notReady) ||
				(tryN == 1 && notReady && connState != connectivity.Connecting) {
				continue
			}
			return walleapi.NewWalleApiClient(conn), nil
		}
	}
	return nil, ErrConnUnavailable
}

func (c *client) ForServer(serverId string) (walle_pb.WalleClient, error) {
	c.mx.Lock()
	defer c.mx.Unlock()
	conn, err := c.unsafeServerConn(serverId)
	if err != nil {
		return nil, err
	}
	if conn.GetState() == connectivity.TransientFailure {
		return nil, ErrConnUnavailable
	}
	return walle_pb.NewWalleClient(conn), nil
}

func (c *client) unsafeServerConn(serverId string) (*grpc.ClientConn, error) {
	conn, ok := c.conns[serverId]
	if !ok {
		serverInfo, ok := c.topology.Servers[serverId]
		if !ok {
			return nil, errors.Errorf("serverId: %s, not found in topology", serverId)
		}
		var err error
		conn, err = grpc.Dial(
			serverInfo.Address,
			grpc.WithInsecure(), // TODO(zviad): Decide what to do about security...
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay: connectTimeout,
					// WALLE is a critical low-latency service, thus it is better to err on the side
					// of retrying to connect too often, rather than backoff for too long.
					MaxDelay:   ReconnectDelay,
					Multiplier: 1.6,
					Jitter:     0.2,
				},
			}))
		if err != nil {
			return nil, err
		}
		c.conns[serverId] = conn
	}
	return conn, nil
}
