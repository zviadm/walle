package topomgr

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *Manager) UpdateServerInfo(
	ctx context.Context,
	req *topomgr.UpdateServerInfoRequest) (*empty.Empty, error) {
	putCtx, err := m.updateServerInfo(req)
	if err := resolvePutCtx(ctx, putCtx, err); err != nil {
		return nil, err
	}
	zlog.Infof(
		"[tm] updated server: %s : %s -> %s",
		req.ClusterUri, hex.EncodeToString([]byte(req.ServerId)), req.ServerInfo)
	return &empty.Empty{}, nil
}

func (m *Manager) updateServerInfo(req *topomgr.UpdateServerInfoRequest) (*wallelib.PutCtx, error) {
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
	req *topomgr.UpdateServerIdsRequest) (*topomgr.UpdateServerIdsResponse, error) {
	if err := storage.IsValidStreamURI(req.StreamUri); err != nil {
		return nil, err
	}

	c, unlock, err := m.clusterMX(req.ClusterUri)
	if err != nil {
		return nil, err
	}
	topology := c.topology
	unlock()

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
	return &topomgr.UpdateServerIdsResponse{}, nil
}

func (m *Manager) waitForStreamVersion(
	ctx context.Context, t *walleapi.Topology, streamURI string) error {
	streamVersion := t.Streams[streamURI].GetVersion()
	nServerIds := len(t.Streams[streamURI].GetServerIds())
	if streamVersion == 0 || nServerIds == 0 {
		return nil
	}
	c := wallelib.NewClient(ctx, &wallelib.StaticDiscovery{T: t})
	return wallelib.KeepTryingWithBackoff(ctx, wallelib.LeaseMinimum, time.Second,
		func(retryN uint) (bool, bool, error) {
			streamC, err := c.ForStream(streamURI)
			if err != nil {
				return true, false, err
			}
			wStatus, err := streamC.WriterStatus(ctx, &walleapi.WriterStatusRequest{StreamUri: streamURI})
			if err != nil {
				return (retryN >= 2), false, err
			}
			if wStatus.StreamVersion != streamVersion {
				return (retryN >= 2), false, status.Errorf(codes.Unavailable,
					"servers for %s don't have up-to-date stream version: %d < %d",
					streamURI, wStatus.StreamVersion, streamVersion)
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
	if streamVersion == 0 {
		streamT = &walleapi.StreamTopology{}
		topology.Streams[req.StreamUri] = streamT
	}
	streamT.Version += 1
	streamT.ServerIds = req.ServerIds
	putCtx, err := c.commitTopology(topology)
	return putCtx, topology, err
}
