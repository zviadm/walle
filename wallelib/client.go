// Package wallelib provides client libraries to interact with WALLE servers.
//
// Simple exclusive "writer" usage:
//
//		rootPb, err := RootPbFromEnv()
//		// ... handle error
// 		c, err := NewClientFromRootPb(ctx, rootPb, ...)
//		// ... handle error
//		w, err := WaitAndClaim(ctx, c, ...)
//		// ... handle error
//		// `w` can be used to check on last committed entry and write new entries
//		// in the stream as an exclusive writer.
package wallelib

import (
	"context"
	"sort"
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

// Client represents WALLE client that is pointing to a single cluster.
type Client interface {
	// Returns gRPC client to talk to specific streamURI within a cluster.
	ForStream(streamURI string) (walleapi.WalleApiClient, error)
}

type client struct {
	d Discovery

	mx        sync.Mutex
	topology  *walleapi.Topology
	conns     map[string]*grpc.ClientConn // serverId -> conn
	serverIds map[string][]string         // streamURI -> []serverId
	preferred map[string][]string         // streamURI -> []serverId
	rrIdx     map[string]int              // streamURI -> round-robin index
}

// ErrConnUnavailable is returned when all connections are in TransientFailure state.
var ErrConnUnavailable = status.Error(codes.Unavailable, "connection in TransientFailure")

// NewClient creates new client given discovery for WALLE cluster.
func NewClient(ctx context.Context, d Discovery) *client {
	c := &client{
		d:         d,
		conns:     make(map[string]*grpc.ClientConn),
		preferred: make(map[string][]string),
		rrIdx:     make(map[string]int),
	}
	topology, notify := d.Topology()
	c.update(topology)
	go c.watcher(ctx, notify)
	return c
}

// NewClientFromRootPb is a convenience method for creating new client
// given root cluster topology protobuf and clusterURI string only. Will create
// discoveries for root cluster and clusterURI internally.
func NewClientFromRootPb(
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string) (*client, error) {
	rootD, err := NewRootDiscovery(ctx, rootPb, true)
	if err != nil {
		return nil, err
	}
	rootC := NewClient(ctx, rootD)
	if clusterURI == "" || clusterURI == rootPb.RootUri {
		return rootC, nil
	}
	d, err := NewDiscovery(ctx, rootC, clusterURI, nil)
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
			c.close()
			return
		}
		topology, notify = c.d.Topology()
		c.update(topology)
	}
}

func (c *client) close() {
	c.mx.Lock()
	defer c.mx.Unlock()
	for serverId, conn := range c.conns {
		conn.Close()
		delete(c.conns, serverId)
	}
}

func (c *client) update(topology *walleapi.Topology) {
	serverIds := make(map[string][]string, len(topology.Streams))
	for streamURI, streamT := range topology.Streams {
		sIds := append(serverIds[streamURI], streamT.ServerIds...)
		sort.Sort(sort.StringSlice(sIds))
		serverIds[streamURI] = sIds
	}

	c.mx.Lock()
	defer c.mx.Unlock()
	c.topology = topology
	c.serverIds = serverIds
	// Close and clear out all connections to serverIds that are no longer registered in topology.
	for serverId, conn := range c.conns {
		info, ok := topology.Servers[serverId]
		if !ok || info.Address != conn.Target() {
			conn.Close()
			delete(c.conns, serverId)
		}
	}
	for streamURI := range c.preferred {
		if _, ok := c.serverIds[streamURI]; !ok {
			delete(c.preferred, streamURI)
		}
	}
	for streamURI := range c.rrIdx {
		if _, ok := c.serverIds[streamURI]; !ok {
			delete(c.rrIdx, streamURI)
		}
	}
}

func (c *client) ForStream(streamURI string) (walleapi.WalleApiClient, error) {
	c.mx.Lock()
	defer c.mx.Unlock()
	serverIds, ok := c.serverIds[streamURI]
	if !ok {
		return nil, errors.Errorf("streamURI: %s, not found in topology", streamURI)
	}
	preferredIds := c.preferred[streamURI]
	offset := c.rrIdx[streamURI]
	c.rrIdx[streamURI] = offset + 1
	for tryN := 0; tryN < 2; tryN++ {
		for i := 0; i < len(preferredIds)+len(serverIds); i++ {
			var serverId string
			isPreferred := i < len(preferredIds)
			if isPreferred {
				serverId = preferredIds[(offset+i)%len(preferredIds)]
			} else {
				serverId = serverIds[(offset+i)%len(serverIds)]
			}
			conn, err := c.serverConn(serverId)
			if err != nil {
				continue
			}
			connState := conn.GetState()
			notReady := connState != connectivity.Ready && connState != connectivity.Idle
			if (tryN == 0 && notReady) ||
				(tryN == 1 && notReady && connState != connectivity.Connecting) {
				continue
			}
			return &wrapClient{
				WalleApiClient: walleapi.NewWalleApiClient(conn),
				c:              c,
				serverId:       serverId,
			}, nil
		}
	}
	return nil, ErrConnUnavailable
}

type wrapClient struct {
	walleapi.WalleApiClient
	c        *client
	serverId string
}

func (w *wrapClient) PutEntry(
	ctx context.Context,
	in *walleapi.PutEntryRequest,
	opts ...grpc.CallOption) (*walleapi.PutEntryResponse, error) {
	resp, err := w.WalleApiClient.PutEntry(ctx, in, opts...)
	if err != nil {
		w.c.removePreferred(in.StreamUri, w.serverId)
	} else {
		w.c.updatePreferred(in.StreamUri, resp.ServerIds)
	}
	return resp, err
}

func (c *client) removePreferred(streamURI string, serverId string) {
	c.mx.Lock()
	defer c.mx.Unlock()
	for idx, sId := range c.preferred[streamURI] {
		if sId == serverId {
			c.preferred[streamURI] = append(
				c.preferred[streamURI][:idx], c.preferred[streamURI][idx+1:]...)
			return
		}
	}
}
func (c *client) updatePreferred(streamURI string, serverIds []string) {
	c.mx.Lock()
	defer c.mx.Unlock()
	preferredIds := append(c.preferred[streamURI][:0], serverIds...)
	sort.Sort(sort.StringSlice(preferredIds))
	c.preferred[streamURI] = preferredIds
}

func (c *client) ForServer(serverId string) (walle_pb.WalleClient, error) {
	c.mx.Lock()
	defer c.mx.Unlock()
	conn, err := c.serverConn(serverId)
	if err != nil {
		return nil, err
	}
	if conn.GetState() == connectivity.TransientFailure {
		return nil, ErrConnUnavailable
	}
	return walle_pb.NewWalleClient(conn), nil
}

// serverConn returns connection for given serverId.
// assumes c.mx is locked.
func (c *client) serverConn(serverId string) (*grpc.ClientConn, error) {
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

// IsErrFinal returns True for error codes that are never retriable in WALLE.
func IsErrFinal(errCode codes.Code) bool {
	return errCode == codes.FailedPrecondition || errCode == codes.InvalidArgument
}
