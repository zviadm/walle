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
		req.TopologyUri, hex.EncodeToString([]byte(req.ServerId)), req.ServerInfo)
	return &empty.Empty{}, nil
}

func (m *Manager) updateServerInfo(req *topomgr.UpdateServerInfoRequest) (*wallelib.PutCtx, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer unlock()

	if proto.Equal(p.topology.Servers[req.ServerId], req.ServerInfo) {
		return p.putCtx, nil
	}
	p.topology.Version += 1
	p.topology.Servers[req.ServerId] = req.ServerInfo
	return p.commitTopology(), nil
}

func (m *Manager) FetchTopology(
	ctx context.Context,
	req *topomgr.FetchTopologyRequest) (*walleapi.Topology, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	putCtx := p.putCtx
	topology := proto.Clone(p.topology).(*walleapi.Topology)
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

	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	topology := proto.Clone(p.topology).(*walleapi.Topology)
	unlock()

	changed, err := verifyAndDiffMembershipChange(topology, req.StreamUri, req.ServerIds)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	// First make sure majority of current members are at the latest version.
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
			streamC, err := c.ForStream(streamURI, -1)
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
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	if p.topology.Streams[req.StreamUri].GetVersion() != streamVersion {
		return nil, nil, status.Errorf(codes.Unavailable, "conflict with concurrent topology update for: %s", req.StreamUri)
	}
	p.topology.Version += 1
	streamT := p.topology.Streams[req.StreamUri]
	if streamVersion == 0 {
		streamT = &walleapi.StreamTopology{}
		p.topology.Streams[req.StreamUri] = streamT
	}
	streamT.Version += 1
	streamT.ServerIds = req.ServerIds
	topology := proto.Clone(p.topology).(*walleapi.Topology)
	putCtx := p.commitTopology()
	return putCtx, topology, nil
}

func (m *Manager) perTopoMX(topologyURI string) (p *perTopoData, unlock func(), err error) {
	m.mx.Lock()
	defer func() {
		if err != nil {
			m.mx.Unlock()
		}
	}()
	p, ok := m.perTopo[topologyURI]
	if !ok || p.writer == nil {
		return nil, nil, status.Errorf(codes.Unavailable, "not serving: %s", topologyURI)
	}
	writerState, _ := p.writer.WriterState()
	if writerState != wallelib.Exclusive {
		return nil, nil, status.Errorf(codes.Unavailable,
			"writer no longer exclusive: %s - %s", topologyURI, writerState)
	}
	return p, m.mx.Unlock, err
}

func (p *perTopoData) commitTopology() *wallelib.PutCtx {
	topologyB, err := p.topology.Marshal()
	if err != nil {
		panic(err) // this must never happen, crashing is the only sane solution.
	}
	putCtx := p.writer.PutEntry(topologyB)
	p.putCtx = putCtx
	return putCtx
}

func resolvePutCtx(ctx context.Context, putCtx *wallelib.PutCtx, err error) error {
	if err != nil {
		return err
	}
	if putCtx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-putCtx.Done():
		return putCtx.Err()
	}
}
