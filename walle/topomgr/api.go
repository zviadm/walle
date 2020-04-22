package topomgr

import (
	"context"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegisterServer implements topomgr.TomoManagerServer interface.
func (m *Manager) RegisterServer(
	ctx context.Context,
	req *topomgr.RegisterServerRequest) (*walleapi.Empty, error) {
	putCtx, err := m.registerServer(req)
	if err := resolvePutCtx(ctx, putCtx, err); err != nil {
		return nil, err
	}
	zlog.Infof(
		"[tm] updated server: %s : %s -> %s", req.ClusterUri, req.ServerId, req.ServerInfo)
	return &walleapi.Empty{}, nil
}

func (m *Manager) registerServer(req *topomgr.RegisterServerRequest) (*wallelib.PutCtx, error) {
	c, unlock, err := m.clusterMX(req.ClusterUri)
	if err != nil {
		return nil, err
	}
	defer unlock()

	if proto.Equal(c.topology.Servers[req.ServerId], req.ServerInfo) {
		return c.putCtx, nil
	}
	topology := proto.Clone(c.topology).(*walleapi.Topology)
	topology.Version += 1
	if topology.Servers == nil {
		topology.Servers = make(map[string]*walleapi.ServerInfo, 1)
	}
	topology.Servers[req.ServerId] = req.ServerInfo
	return c.commitTopology(topology)
}

// FetchTopology implements topomgr.TomoManagerServer interface.
func (m *Manager) FetchTopology(
	ctx context.Context,
	req *topomgr.FetchTopologyRequest) (*walleapi.Topology, error) {
	c, unlock, err := m.clusterMX(req.ClusterUri)
	if err != nil {
		return nil, err
	}
	putCtx := c.putCtx
	topology := c.topology
	unlock()

	if err := resolvePutCtx(ctx, putCtx, nil); err != nil {
		return nil, err
	}
	return topology, nil
}

// CrUpdateStream implements topomgr.TomoManagerServer interface.
func (m *Manager) CrUpdateStream(
	ctx context.Context,
	req *topomgr.CrUpdateStreamRequest) (*walleapi.Empty, error) {
	if err := storage.ValidateStreamURI(req.StreamUri); err != nil {
		return nil, err
	}

	c, unlock, err := m.clusterMX(req.ClusterUri)
	if err != nil {
		return nil, err
	}
	topology := c.topology
	unlock()
	if strings.HasPrefix(req.StreamUri, Prefix) && topology.RootUri == "" {
		return nil, status.Error(
			codes.InvalidArgument, "/cluster streams can only exist in root cluster")
	}

	changed, err := verifyAndDiffMembershipChange(topology, req.StreamUri, req.ServerIds)
	if err != nil {
		return nil, err
	}
	topologyNew := c.topology.Streams[req.StreamUri]
	if changed {
		if topologyNew == nil {
			topologyNew = &walleapi.StreamTopology{}
		} else {
			topologyNew = proto.Clone(topologyNew).(*walleapi.StreamTopology)
		}
		topologyNew.Version += 1
		topologyNew.ServerIds = req.ServerIds
	}
	if err := m.updateStreamTopology(
		ctx, req.ClusterUri, req.StreamUri, topology, topologyNew); err != nil {
		return nil, err
	}
	return &walleapi.Empty{}, nil
}

// TrimStream implements topomgr.TopoManagerServer interface.
func (m *Manager) TrimStream(
	ctx context.Context,
	req *topomgr.TrimStreamRequest) (*walleapi.Empty, error) {
	c, unlock, err := m.clusterMX(req.ClusterUri)
	if err != nil {
		return nil, err
	}
	topology := c.topology
	unlock()

	topologyNew := topology.Streams[req.StreamUri]
	if topologyNew.GetVersion() == 0 {
		return nil, status.Errorf(codes.NotFound, "stream: %s not found", req.StreamUri)
	}
	if topologyNew.FirstEntryId < req.EntryId {
		cc, err := wallelib.NewClient(
			ctx, &wallelib.StaticDiscovery{T: topology}).ForStream(req.StreamUri)
		if err != nil {
			return nil, err
		}
		stream, err := cc.StreamEntries(ctx, &walleapi.StreamEntriesRequest{
			StreamUri:    req.StreamUri,
			StartEntryId: req.EntryId,
			EndEntryId:   req.EntryId + 1,
		})
		if err != nil {
			return nil, err
		}
		entry, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if entry.EntryId != req.EntryId {
			return nil, status.Errorf(
				codes.Internal, "stream: %s entryId mismatch: %d != %d",
				req.StreamUri, req.EntryId, entry.EntryId)
		}

		topologyNew = proto.Clone(topologyNew).(*walleapi.StreamTopology)
		topologyNew.Version += 1
		topologyNew.FirstEntryId = req.EntryId
		topologyNew.FirstEntryXX = entry.ChecksumXX
	}

	if err := m.updateStreamTopology(
		ctx, req.ClusterUri, req.StreamUri, topology, topologyNew); err != nil {
		return nil, err
	}
	return &walleapi.Empty{}, nil
}
