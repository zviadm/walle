package topomgr

import (
	"context"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/broadcast"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *Manager) RegisterServer(
	ctx context.Context,
	req *topomgr.RegisterServerRequest) (*empty.Empty, error) {
	putCtx, err := m.registerServer(req)
	if err := resolvePutCtx(ctx, putCtx, err); err != nil {
		return nil, err
	}
	zlog.Infof(
		"[tm] updated server: %s : %s -> %s", req.ClusterUri, req.ServerId, req.ServerInfo)
	return &empty.Empty{}, nil
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

func (m *Manager) UpdateServerIds(
	ctx context.Context,
	req *topomgr.UpdateServerIdsRequest) (*empty.Empty, error) {
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
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	// First make sure majority of current members are at the latest version.
	// TODO(zviad): Need to also make sure that there are no GAPs that can lead to data loss
	// when removing serverIds from a member list.
	if err := m.waitForStreamVersion(ctx, topology, req.StreamUri); err != nil {
		return nil, err
	}
	if changed {
		putCtx, topology, err := m.updateServerIds(
			ctx, req, topology.Streams[req.StreamUri].GetVersion())
		if err := resolvePutCtx(ctx, putCtx, err); err != nil {
			return nil, err
		}
		if err := m.waitForStreamVersion(ctx, topology, req.StreamUri); err != nil {
			return nil, err
		}
	}
	return &empty.Empty{}, nil
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

func (m *Manager) updateServerIds(
	ctx context.Context,
	req *topomgr.UpdateServerIdsRequest,
	streamVersion int64) (*wallelib.PutCtx, *walleapi.Topology, error) {
	c, unlock, err := m.clusterMX(req.ClusterUri)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	if c.topology.Streams[req.StreamUri].GetVersion() != streamVersion {
		return nil, nil, status.Errorf(codes.Unavailable, "conflict with concurrent topology update for: %s", req.StreamUri)
	}
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
	streamT.Version += 1
	streamT.ServerIds = req.ServerIds
	putCtx, err := c.commitTopology(topology)
	return putCtx, topology, err
}
