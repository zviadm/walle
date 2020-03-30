package topomgr

import (
	"context"
	"encoding/hex"
	"reflect"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
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
	defer unlock()
	entry := p.writer.Committed()
	topology, err := wallelib.TopologyFromEntry(entry)
	if err != nil {
		return nil, err
	}
	return topology, nil
}

func (m *Manager) UpdateServerIds(
	ctx context.Context,
	req *topomgr.UpdateServerIdsRequest) (*topomgr.UpdateServerIdsResponse, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	prevEquals := reflect.DeepEqual(
		p.topology.Streams[req.StreamUri].GetServerIds(),
		p.topology.Streams[req.StreamUri].GetPrevServerIds())
	requiredStreamVersion := p.topology.Streams[req.StreamUri].GetVersion()
	changed, err := verifyAndDiffMembershipChange(p.topology, req.StreamUri, req.ServerIds)
	if err != nil {
		unlock()
		if err != nil {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
	}
	if !changed && prevEquals {
		topologyVersion := p.topology.Version
		putCtx := p.putCtx
		unlock()
		if err := resolvePutCtx(ctx, putCtx, nil); err != nil {
			return nil, err
		}
		return &topomgr.UpdateServerIdsResponse{
			TopologyVersion: topologyVersion,
			StreamVersion:   requiredStreamVersion}, nil
	}
	unlock()

	nUpdates := 1
	if changed {
		nUpdates += 1
	}
	var resp *topomgr.UpdateServerIdsResponse
	var putCtx *wallelib.PutCtx
	for i := 0; i < nUpdates; i++ {
		if requiredStreamVersion > 0 {
			if err := m.waitForStreamVersion(
				ctx, req.StreamUri, requiredStreamVersion); err != nil {
				return nil, err
			}
		}
		resp, putCtx, err = m.updateServerIds(ctx, req, requiredStreamVersion)
		if err := resolvePutCtx(ctx, putCtx, err); err != nil {
			return nil, err
		}
		requiredStreamVersion = resp.StreamVersion
	}
	return resp, nil
}

func (m *Manager) waitForStreamVersion(
	ctx context.Context, streamURI string, streamVersion int64) error {
	return wallelib.KeepTryingWithBackoff(ctx, wallelib.LeaseMinimum, time.Second,
		func(retryN uint) (bool, bool, error) {
			streamC, err := m.c.ForStream(streamURI, -1)
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
	requiredStreamVersion int64) (*topomgr.UpdateServerIdsResponse, *wallelib.PutCtx, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	if requiredStreamVersion > 0 && p.topology.Streams[req.StreamUri].Version != requiredStreamVersion {
		return nil, nil, status.Errorf(codes.Unavailable, "conflict with concurrent topology update for: %s", req.StreamUri)
	}
	p.topology.Version += 1
	streamT := p.topology.Streams[req.StreamUri]
	if requiredStreamVersion == 0 {
		streamT = &walleapi.StreamTopology{ServerIds: req.ServerIds}
		p.topology.Streams[req.StreamUri] = streamT
	}
	streamT.Version += 1
	streamT.PrevServerIds = streamT.ServerIds
	streamT.ServerIds = req.ServerIds
	r := &topomgr.UpdateServerIdsResponse{
		TopologyVersion: p.topology.Version,
		StreamVersion:   streamT.Version,
	}
	putCtx := p.commitTopology()
	return r, putCtx, nil
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
