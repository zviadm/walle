package topomgr

import (
	"context"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/broadcast"
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

	// First make sure majority of current members are at the latest version.
	// TODO(zviad): Need to also make sure that there are no GAPs that can lead to data loss
	// when removing serverIds from a member list.
	if err := m.waitForStreamVersion(ctx, topology, req.StreamUri); err != nil {
		return nil, err
	}
	if changed {
		prevServerIds := topology.Streams[req.StreamUri].GetServerIds()
		topology := proto.Clone(c.topology).(*walleapi.Topology)
		topology.Version += 1
		streamT := topology.Streams[req.StreamUri]
		if streamT == nil {
			streamT = &walleapi.StreamTopology{}
			if topology.Streams == nil {
				topology.Streams = make(map[string]*walleapi.StreamTopology, 1)
			}
			topology.Streams[req.StreamUri] = streamT
		}
		streamT.ServerIds = req.ServerIds
		if len(prevServerIds) != 1 {
			// Other than special case of going from 1 -> 2 nodes, it is important
			// to make sure that majority in new member set are also at the latest version
			// already.
			if err := m.waitForStreamVersion(ctx, topology, req.StreamUri); err != nil {
				return nil, err
			}
		}
		streamT.Version += 1

		putCtx, err := m.crUpdateStream(
			ctx, req.ClusterUri, req.StreamUri, topology)
		if err := resolvePutCtx(ctx, putCtx, err); err != nil {
			return nil, err
		}
		if err := m.waitForStreamVersion(ctx, topology, req.StreamUri); err != nil {
			return nil, err
		}
		zlog.Infof(
			"[tm] updated members: %s : %s -> %s", req.StreamUri, prevServerIds, req.ServerIds)
	}
	return &walleapi.Empty{}, nil
}

func (m *Manager) waitForStreamVersion(
	ctx context.Context, t *walleapi.Topology, streamURI string) error {
	streamVersion := t.Streams[streamURI].GetVersion()
	nServerIds := len(t.Streams[streamURI].GetServerIds())
	if streamVersion == 0 || nServerIds == 0 {
		return nil
	}
	serverId := t.Streams[streamURI].ServerIds[0]
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := wallelib.NewClient(ctx, &wallelib.StaticDiscovery{T: t})
	return wallelib.KeepTryingWithBackoff(ctx, wallelib.LeaseMinimum, time.Second,
		func(retryN uint) (bool, bool, error) {
			wInfo, err := broadcast.WriterInfo(ctx, c, serverId, streamURI, t.Streams[streamURI])
			if err != nil {
				return (retryN >= 2), false, err
			}
			if wInfo.StreamVersion != streamVersion {
				return (retryN >= 2), false, status.Errorf(codes.Unavailable,
					"servers for %s don't have up-to-date stream version: %d < %d",
					streamURI, wInfo.StreamVersion, streamVersion)
			}
			return true, false, nil
		})
}

func (m *Manager) crUpdateStream(
	ctx context.Context,
	clusterURI string,
	streamURI string,
	topologyNew *walleapi.Topology) (*wallelib.PutCtx, error) {
	c, unlock, err := m.clusterMX(clusterURI)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if c.topology.Streams[streamURI].GetVersion() != topologyNew.Streams[streamURI].GetVersion()-1 {
		return nil, status.Errorf(codes.Aborted, "conflict with concurrent topology update for: %s", streamURI)
	}
	putCtx, err := c.commitTopology(topologyNew)
	return putCtx, err
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
	if entry.ChecksumXX != req.EntryXX {
		return nil, status.Errorf(
			codes.FailedPrecondition, "stream: %s:%d entryXX mismatch: %d != %d",
			req.StreamUri, req.EntryId, entry.ChecksumXX, req.EntryXX)
	}
	putCtx, err := m.trimStream(ctx, req)
	if err := resolvePutCtx(ctx, putCtx, err); err != nil {
		return nil, err
	}
	return &walleapi.Empty{}, nil
}

func (m *Manager) trimStream(
	ctx context.Context,
	req *topomgr.TrimStreamRequest) (*wallelib.PutCtx, error) {
	c, unlock, err := m.clusterMX(req.ClusterUri)
	if err != nil {
		return nil, err
	}
	defer unlock()
	streamT := c.topology.Streams[req.StreamUri]
	if streamT.FirstEntryId == req.EntryId {
		return nil, nil
	}
	if streamT.FirstEntryId > req.EntryId {
		return nil, status.Errorf(
			codes.FailedPrecondition, "stream: %s is already trimmed to: %d > %d",
			req.StreamUri, streamT.FirstEntryId, req.EntryId)
	}
	topology := proto.Clone(c.topology).(*walleapi.Topology)
	topology.Streams[req.StreamUri].Version += 1
	topology.Streams[req.StreamUri].FirstEntryId = req.EntryId
	topology.Streams[req.StreamUri].FirstEntryXX = req.EntryXX
	putCtx, err := c.commitTopology(topology)
	return putCtx, err
}
