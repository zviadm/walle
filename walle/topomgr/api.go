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
	if p.topology.Servers == nil {
		p.topology.Servers = make(map[string]*walleapi.ServerInfo, 1)
	}
	p.topology.Servers[req.ServerId] = req.ServerInfo
	return p.updateTopology(), nil
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

func (m *Manager) UpdateTopology(
	ctx context.Context,
	req *topomgr.UpdateTopologyRequest) (*empty.Empty, error) {
	updateErr, err := m.updateTopology(ctx, req)
	if err := resolveUpdateErr(ctx, updateErr, err); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (m *Manager) updateTopology(
	ctx context.Context,
	req *topomgr.UpdateTopologyRequest) (<-chan error, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if p.topology.GetVersion()+1 != req.Topology.Version {
		return nil, status.Errorf(codes.FailedPrecondition,
			"topology version mismatch: %s - %d + 1 != %d",
			req.TopologyUri, p.topology.GetVersion(), req.Topology.Version)
	}
	p.topology = req.Topology
	return p.updateTopology(), nil
}

func (m *Manager) UpdateServerIds(
	ctx context.Context,
	req *topomgr.UpdateServerIdsRequest) (*empty.Empty, error) {
	updateErr, err := m.updateServerIds(ctx, req)
	if err := resolveUpdateErr(ctx, updateErr, err); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (m *Manager) updateServerIds(
	ctx context.Context,
	req *topomgr.UpdateServerIdsRequest) (<-chan error, error) {
	p, unlock, err := m.perTopoMX(req.TopologyUri)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return p.updateTopology(), nil
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
		return nil, nil, status.Errorf(codes.FailedPrecondition, "not serving: %s", topologyURI)
	}
	writerState, _ := p.writer.WriterState()
	if writerState != wallelib.Exclusive {
		return nil, nil, status.Errorf(codes.FailedPrecondition,
			"writer no longer exclusive: %s - %s", topologyURI, writerState)
	}
	return p, m.mx.Unlock, err
}

func (p *perTopoData) updateTopology() <-chan error {
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
