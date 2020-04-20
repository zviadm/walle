package server

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
)

type mockSystem struct {
	storagePath string
	d           *mockDiscovery

	servers    map[string]*Server
	mx         sync.Mutex
	isDisabled map[string]bool
}

type mockDiscovery struct {
	mx sync.Mutex
	t  *walleapi.Topology
	n  chan struct{}
}

func (t *mockDiscovery) Topology() (*walleapi.Topology, <-chan struct{}) {
	t.mx.Lock()
	defer t.mx.Unlock()
	if t.n == nil {
		t.n = make(chan struct{})
	}
	return t.t, t.n
}
func (t *mockDiscovery) update(topo *walleapi.Topology) {
	t.mx.Lock()
	defer t.mx.Unlock()
	t.t = topo
	if t.n != nil {
		close(t.n)
		t.n = nil
	}
}

func newMockSystem(
	ctx context.Context,
	topology *walleapi.Topology,
	storagePath string) (*mockSystem, *mockClient) {
	m := &mockSystem{
		storagePath: storagePath,
		servers:     make(map[string]*Server, len(topology.Servers)),
		isDisabled:  make(map[string]bool, len(topology.Servers)),
	}
	apiClient := &mockApiClient{m}
	client := &mockClient{apiClient, m}
	m.d = &mockDiscovery{t: topology}

	// Perform expensive part without lock.
	storages := make(map[string]storage.Storage, len(topology.Servers))
	for serverId := range topology.Servers {
		var err error
		storages[serverId], err = storage.Init(
			path.Join(storagePath, serverId+".walledb"),
			storage.InitOpts{Create: true, ServerId: serverId})
		panic.OnErr(err)
	}
	m.mx.Lock()
	defer m.mx.Unlock()
	for serverId := range topology.Servers {
		m.servers[serverId] = New(
			ctx, storages[serverId], client, m.d, nil)
	}
	go func() {
		<-ctx.Done()
		// Need to sleep before cleaning up storage, since not everything exits immediatelly
		// and can cause a crash.
		time.Sleep(wallelib.LeaseMinimum)
		m.cleanup()
	}()
	return m, client
}

func (m *mockSystem) cleanup() {
	m.mx.Lock()
	defer m.mx.Unlock()
	for _, s := range m.servers {
		<-s.s.CloseC()
	}
	m.servers = nil
	_ = os.RemoveAll(m.storagePath)
}

func (m *mockSystem) Server(serverId string) (*Server, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.isDisabled[serverId] {
		return nil, errors.Errorf("[%s] is unavailable!", serverId)
	}
	s, ok := m.servers[serverId]
	if !ok {
		return nil, errors.Errorf("[%s] doesn't exist!", serverId)
	}
	return s, nil
}

func (m *mockSystem) RandServer() (*Server, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	for serverId, s := range m.servers {
		if m.isDisabled[serverId] {
			continue
		}
		return s, nil
	}
	return nil, errors.Errorf("no servers available!")
}

func (m *mockSystem) Toggle(serverId string, enabled bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	_, ok := m.servers[serverId]
	panic.OnNotOk(ok, fmt.Sprintf("unknown serverId: %s", serverId))
	m.isDisabled[serverId] = !enabled
}

type mockClient struct {
	*mockApiClient
	m *mockSystem
}

func (m *mockClient) ForServer(serverId string) (walle_pb.WalleClient, error) {
	s, err := m.m.Server(serverId)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errors.Errorf("unknown serverId: %s", serverId)
	}
	return &clientSelf{s: s}, nil
}

type mockApiClient struct {
	m *mockSystem
}

func (m *mockApiClient) ForStream(streamURI string) (walleapi.WalleApiClient, error) {
	return m, nil
}

func (m *mockApiClient) ClaimWriter(
	ctx context.Context,
	in *walleapi.ClaimWriterRequest,
	opts ...grpc.CallOption) (*walleapi.ClaimWriterResponse, error) {
	s, err := m.m.RandServer()
	if err != nil {
		return nil, err
	}
	return s.ClaimWriter(ctx, in)
}
func (m *mockApiClient) WriterStatus(
	ctx context.Context,
	in *walleapi.WriterStatusRequest,
	opts ...grpc.CallOption) (*walleapi.WriterStatusResponse, error) {
	s, err := m.m.RandServer()
	if err != nil {
		return nil, err
	}
	return s.WriterStatus(ctx, in)
}

func (m *mockApiClient) PutEntry(
	ctx context.Context,
	in *walleapi.PutEntryRequest,
	opts ...grpc.CallOption) (*walleapi.PutEntryResponse, error) {
	s, err := m.m.RandServer()
	if err != nil {
		return nil, err
	}
	return s.PutEntry(ctx, in)
}

func (m *mockApiClient) PollStream(
	ctx context.Context,
	in *walleapi.PollStreamRequest,
	opts ...grpc.CallOption) (*walleapi.Entry, error) {
	s, err := m.m.RandServer()
	if err != nil {
		return nil, err
	}
	return s.PollStream(ctx, in)
}

func (m *mockApiClient) StreamEntries(
	ctx context.Context,
	in *walleapi.StreamEntriesRequest,
	opts ...grpc.CallOption) (walleapi.WalleApi_StreamEntriesClient, error) {
	s, err := m.m.RandServer()
	if err != nil {
		return nil, err
	}
	sClient, sServer, closeF := newEntriesStreams(ctx)
	go func() {
		err := s.StreamEntries(in, sServer)
		closeF(err)
	}()
	return sClient, nil
}
