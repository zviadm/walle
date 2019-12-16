package walle

import (
	"context"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
)

type mockSystem struct {
	walle_pb.WalleClient

	mx         sync.Mutex
	servers    map[string]*Server
	isDisabled map[string]bool
}

func newMockSystem(ctx context.Context, serverIds []string) (*mockSystem, *mockApiClient) {
	mSystem := &mockSystem{
		servers:    make(map[string]*Server, len(serverIds)),
		isDisabled: make(map[string]bool, len(serverIds)),
	}
	mClient := &mockClient{mSystem}
	for _, serverId := range serverIds {
		m := newMockStorage(serverId, []string{"/mock/1"}, serverIds)
		mSystem.servers[serverId] = NewServer(ctx, serverId, m, mClient)
	}
	return mSystem, &mockApiClient{mSystem}
}

func (m *mockSystem) Server(serverId string) (*Server, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.isDisabled[serverId] {
		return nil, errors.Errorf("[%s] is unavailable!", serverId)
	}
	return m.servers[serverId], nil
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
	if !ok {
		panic(fmt.Sprintf("unknown serverId: %s", serverId))
	}
	m.isDisabled[serverId] = !enabled
}

type mockClient struct {
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
	return m, nil
}

func (m *mockClient) PutEntryInternal(
	ctx context.Context,
	in *walle_pb.PutEntryInternalRequest,
	opts ...grpc.CallOption) (*walle_pb.PutEntryInternalResponse, error) {
	s, err := m.m.Server(in.ServerId)
	if err != nil {
		return nil, err
	}
	return s.PutEntryInternal(ctx, in)
}

func (m *mockClient) NewWriter(
	ctx context.Context,
	in *walle_pb.NewWriterRequest,
	opts ...grpc.CallOption) (*walle_pb.NewWriterResponse, error) {
	s, err := m.m.Server(in.ServerId)
	if err != nil {
		return nil, err
	}
	return s.NewWriter(ctx, in)
}

func (m *mockClient) LastEntries(
	ctx context.Context,
	in *walle_pb.LastEntriesRequest,
	opts ...grpc.CallOption) (*walle_pb.LastEntriesResponse, error) {
	s, err := m.m.Server(in.ServerId)
	if err != nil {
		return nil, err
	}
	return s.LastEntries(ctx, in)
}

func (m *mockClient) ReadEntries(
	ctx context.Context,
	in *walle_pb.ReadEntriesRequest,
	opts ...grpc.CallOption) (walle_pb.Walle_ReadEntriesClient, error) {

	s, err := m.m.Server(in.ServerId)
	if err != nil {
		return nil, err
	}
	sClient, sServer, closeF := newMockReadEntriesStreams(ctx, 2)
	go func() {
		err := s.ReadEntries(in, sServer)
		closeF(err)
	}()
	return sClient, nil
}

func newMockReadEntriesStreams(
	ctx context.Context, bufferSize int) (
	walle_pb.Walle_ReadEntriesClient, walle_pb.Walle_ReadEntriesServer, func(err error)) {
	s := &mockReadEntriesStream{ctx: ctx, buffer: make(chan entryPair, bufferSize)}
	return &mockReadEntriesStreamClient{mockReadEntriesStream: s},
		&mockReadEntriesStreamServer{mockReadEntriesStream: s},
		s.Close
}

type mockReadEntriesStream struct {
	ctx    context.Context
	buffer chan entryPair
}

type mockReadEntriesStreamClient struct {
	*mockReadEntriesStream
	grpc.ClientStream
}
type mockReadEntriesStreamServer struct {
	*mockReadEntriesStream
	grpc.ServerStream
}

type entryPair struct {
	Entry *walleapi.Entry
	Err   error
}

func (m *mockReadEntriesStreamClient) Context() context.Context {
	return m.ctx
}
func (m *mockReadEntriesStreamServer) Context() context.Context {
	return m.ctx
}

func (m *mockReadEntriesStream) Recv() (*walleapi.Entry, error) {
	r := <-m.buffer
	return r.Entry, r.Err
}

func (m *mockReadEntriesStream) Send(e *walleapi.Entry) error {
	m.buffer <- entryPair{Entry: e}
	return nil
}

func (m *mockReadEntriesStream) Close(err error) {
	if err == nil {
		err = io.EOF
	}
	m.buffer <- entryPair{Err: err}
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

func (m *mockApiClient) StreamEntries(
	ctx context.Context,
	req *walleapi.StreamEntriesRequest,
	opts ...grpc.CallOption) (walleapi.WalleApi_StreamEntriesClient, error) {
	return nil, errors.Errorf("not implemented")
}
