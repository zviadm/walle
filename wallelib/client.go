package wallelib

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
)

type BasicClient interface {
	ForStream(streamURI string) (walleapi.WalleApiClient, error)
}

type client struct {
	d Discovery

	mx         sync.Mutex
	topology   *walleapi.Topology
	conns      map[string]*grpc.ClientConn // serverId -> conn
	connHealth map[string]*connHealth      // serverId -> connHealth
	preferred  map[string][]string         // streamURI -> []serverId
}

type connHealth struct {
	ErrN     int64
	DownNano int64
}

var ErrConnUnavailable = errors.New("connection in TransientFailure")

func NewClient(ctx context.Context, d Discovery) *client {
	c := &client{
		d:          d,
		conns:      make(map[string]*grpc.ClientConn),
		connHealth: make(map[string]*connHealth),
	}
	topology, notify := d.Topology()
	c.update(topology)
	go c.watcher(ctx, notify)
	return c
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

		// TODO(zviad): actually sort by preference, instead of randomizing
		// the order.
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
		// TODO(zviad): we should also close connections that now have incorrect targets, if
		// server address has changed.
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
	var oneErr error = ErrConnUnavailable
	preferredMajority := len(preferredIds)/2 + 1
	for idx, serverId := range preferredIds {
		conn, err := c.unsafeServerConn(serverId)
		if err != nil {
			oneErr = err
			continue
		}
		if conn.GetState() == connectivity.TransientFailure {
			continue
		}
		health, ok := c.connHealth[serverId]
		if !ok {
			health = &connHealth{}
			c.connHealth[serverId] = health
		}
		downNano := atomic.LoadInt64(&health.DownNano)
		if downNano > time.Now().UnixNano() {
			continue
		}
		if idx > 0 && idx < preferredMajority {
			copy(preferredIds[1:idx+1], preferredIds[:idx])
			preferredIds[0] = serverId
		}
		return &wApiClient{
			cli:      walleapi.NewWalleApiClient(conn),
			errN:     &health.ErrN,
			downNano: &health.DownNano,
		}, nil
	}
	return nil, errors.Wrapf(
		oneErr, "no server available for: %s", streamURI)
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
	if ok {
		return conn, nil
	}
	serverInfo, ok := c.topology.Servers[serverId]
	if !ok {
		return nil, errors.Errorf("serverId: %s, not found in topology", serverId)
	}
	// TODO(zviad): Decide what to do about security...
	conn, err := grpc.Dial(
		serverInfo.Address,
		grpc.WithInsecure(),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				// TODO(zviad): Make this configurable, for clients that might
				// very large lease minimums.
				BaseDelay:  LeaseMinimum,
				MaxDelay:   120 * time.Second,
				Multiplier: 1.6,
				Jitter:     0.2,
			},
		}))
	if err != nil {
		return nil, err
	}
	return conn, nil
}
