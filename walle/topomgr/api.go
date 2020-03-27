package topomgr

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *Manager) UpdateServerInfo(
	ctx context.Context,
	req *topomgr.UpdateServerInfoRequest) (*empty.Empty, error) {
	updateErr, err := m.updateServerInfo(req)
	if err := resolveUpdateErr(ctx, updateErr, err); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (m *Manager) updateServerInfo(req *topomgr.UpdateServerInfoRequest) (<-chan error, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer unlock()

	if proto.Equal(p.topology.Servers[req.ServerId], req.ServerInfo) {
		errC := make(chan error, 1)
		errC <- nil
		return errC, nil
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
	req *topomgr.UpdateServerIdsRequest) (*empty.Empty, error) {

	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	changed, err := verifyAndDiffMembershipChange(p.topology, req.StreamUri, req.ServerIds)
	if err != nil || !changed {
		unlock()
		return nil, err
	}
	requiredStreamVersion := p.topology.Streams[req.StreamUri].GetVersion()
	unlock()

	if requiredStreamVersion > 0 {
		streamC, err := m.c.ForStream(req.StreamUri)
		if err != nil {
			return nil, err
		}
		wStatus, err := streamC.WriterStatus(ctx, &walleapi.WriterStatusRequest{StreamUri: req.StreamUri})
		if err != nil {
			return nil, err
		}
		if wStatus.StreamVersion != requiredStreamVersion {
			return nil, status.Errorf(codes.Unavailable,
				"servers for %s don't have up-to-date stream version: %d < %d",
				req.StreamUri, wStatus.StreamVersion, requiredStreamVersion)
		}
	}

	updateErr, err := m.updateServerIds(ctx, req, requiredStreamVersion)
	if err := resolveUpdateErr(ctx, updateErr, err); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (m *Manager) updateServerIds(
	ctx context.Context,
	req *topomgr.UpdateServerIdsRequest,
	requiredStreamVersion int64) (<-chan error, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if requiredStreamVersion > 0 && p.topology.Streams[req.StreamUri].Version != requiredStreamVersion {
		return nil, status.Errorf(codes.Unavailable, "conflict with concurrent topology update for: %s", req.StreamUri)
	}
	p.topology.Version += 1
	if requiredStreamVersion == 0 {
		p.topology.Streams[req.StreamUri] = &walleapi.StreamTopology{}
	}
	p.topology.Streams[req.StreamUri].Version += 1
	p.topology.Streams[req.StreamUri].ServerIds = req.ServerIds
	return p.commitTopology(), nil
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

func (p *perTopoData) commitTopology() <-chan error {
	topologyB, err := p.topology.Marshal()
	if err != nil {
		panic(err) // this must never happen, crashing is the only sane solution.
	}
	_, errC := p.writer.PutEntry(topologyB)
	return errC
}

func resolveUpdateErr(ctx context.Context, updateErr <-chan error, err error) error {
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-updateErr:
		return err
	}
}
