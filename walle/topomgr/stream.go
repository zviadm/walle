package topomgr

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/broadcast"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *Manager) updateStreamTopology(
	ctx context.Context,
	clusterURI string,
	streamURI string,
	topology *walleapi.Topology,
	topologyNew *walleapi.StreamTopology) error {

	// First make sure majority of current members are at the latest version.
	// TODO(zviad): Need to also make sure that there are no GAPs that can lead to data loss
	// when removing serverIds from a member list.
	topologyPrev := topology.Streams[streamURI]
	if err := m.waitForStreamVersion(
		ctx, streamURI, topologyPrev.GetServerIds(), topologyPrev.GetVersion(), topology.Servers); err != nil {
		return err
	}
	if proto.Equal(topologyPrev, topologyNew) {
		return nil
	}
	if topologyNew.Version != topologyPrev.GetVersion()+1 {
		return status.Errorf(
			codes.Internal, "invalid stream topology version: %d != %d + 1",
			topologyNew.Version, topologyPrev.GetVersion())
	}

	serverIdsPrev := topologyPrev.GetServerIds()
	if len(serverIdsPrev) != 1 && len(serverIdsPrev) != len(topologyNew.ServerIds) {
		// Other than special case of going from 1 -> 2 nodes, it is important
		// to make sure that majority in new member set are also at the latest version
		// already.
		if err := m.waitForStreamVersion(
			ctx, streamURI, topologyNew.ServerIds, topologyPrev.GetVersion(), topology.Servers); err != nil {
			return err
		}
	}
	putCtx, err := m.commitStreamTopology(
		ctx, clusterURI, streamURI, topologyNew)
	if err := resolvePutCtx(ctx, putCtx, err); err != nil {
		return err
	}
	if err := m.waitForStreamVersion(
		ctx, streamURI, topologyNew.ServerIds, topologyNew.Version, topology.Servers); err != nil {
		return err
	}
	zlog.Infof(
		"[tm] updated stream: %s : %s -> %s", streamURI, topologyPrev, topologyNew)
	return nil
}

func (m *Manager) waitForStreamVersion(
	ctx context.Context,
	streamURI string,
	serverIds []string,
	streamVersion int64,
	servers map[string]*walleapi.ServerInfo) error {
	if streamVersion == 0 || len(serverIds) == 0 {
		return nil
	}
	serverId := serverIds[0]
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	t := &walleapi.Topology{
		Streams: map[string]*walleapi.StreamTopology{
			streamURI: {Version: streamVersion, ServerIds: serverIds},
		},
		Servers: servers,
	}
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

func (m *Manager) commitStreamTopology(
	ctx context.Context,
	clusterURI string,
	streamURI string,
	topologyNew *walleapi.StreamTopology) (*wallelib.PutCtx, error) {
	c, unlock, err := m.clusterMX(clusterURI)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if c.topology.Streams[streamURI].GetVersion()+1 != topologyNew.Version {
		return nil, status.Errorf(codes.Aborted, "conflict with concurrent topology update for: %s", streamURI)
	}
	topology := proto.Clone(c.topology).(*walleapi.Topology)
	topology.Version += 1
	if topology.Streams == nil {
		topology.Streams = make(map[string]*walleapi.StreamTopology)
	}
	topology.Streams[streamURI] = topologyNew
	putCtx, err := c.commitTopology(topology)
	return putCtx, err
}