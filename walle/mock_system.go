package walle

import (
	"context"
	"io"

	"google.golang.org/grpc"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
)

type mockSystem struct {
	walle_pb.WalleClient
	servers map[string]*Server
}

func newMockSystem(serverIds []string) (*mockSystem, *mockApiClient) {
	mSystem := &mockSystem{servers: make(map[string]*Server, len(serverIds))}
	mClient := &mockClient{mSystem}
	for _, serverId := range serverIds {
		m := newMockStorage([]string{"/mock/1"}, serverIds)
		mSystem.servers[serverId] = NewServer(serverId, m, mClient)
	}
	return mSystem, &mockApiClient{mSystem}
}

type mockClient struct {
	m *mockSystem
}

func (m *mockClient) ForServer(serverId string) (walle_pb.WalleClient, error) {
	_, ok := m.m.servers[serverId]
	if !ok {
		return nil, errors.Errorf("unknown serverId: %s", serverId)
	}
	return m, nil
}

func (m *mockClient) PutEntryInternal(
	ctx context.Context,
	in *walle_pb.PutEntryInternalRequest,
	opts ...grpc.CallOption) (*walle_pb.PutEntryInternalResponse, error) {
	return m.m.servers[in.ServerId].PutEntryInternal(ctx, in)
}

func (m *mockClient) NewWriter(
	ctx context.Context,
	in *walle_pb.NewWriterRequest,
	opts ...grpc.CallOption) (*walle_pb.NewWriterResponse, error) {
	return m.m.servers[in.ServerId].NewWriter(ctx, in)
}

func (m *mockClient) LastEntries(
	ctx context.Context,
	in *walle_pb.LastEntriesRequest,
	opts ...grpc.CallOption) (*walle_pb.LastEntriesResponse, error) {
	return m.m.servers[in.ServerId].LastEntries(ctx, in)
}

func (m *mockClient) ReadEntries(
	ctx context.Context,
	in *walle_pb.ReadEntriesRequest,
	opts ...grpc.CallOption) (walle_pb.Walle_ReadEntriesClient, error) {

	sClient, sServer, closeF := newMockReadEntriesStreams(ctx, 2)
	go func() {
		err := m.m.servers[in.ServerId].ReadEntries(in, sServer)
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

func (m *mockApiClient) ForStream(streamURI string) walleapi.WalleApiClient {
	return m
}

func (m *mockApiClient) ClaimWriter(
	ctx context.Context,
	in *walleapi.ClaimWriterRequest,
	opts ...grpc.CallOption) (*walleapi.ClaimWriterResponse, error) {
	for _, s := range m.m.servers {
		return s.ClaimWriter(ctx, in)
	}
	return nil, errors.Errorf("no servers")
}

func (m *mockApiClient) PutEntry(
	ctx context.Context,
	in *walleapi.PutEntryRequest,
	opts ...grpc.CallOption) (*walleapi.PutEntryResponse, error) {
	for _, s := range m.m.servers {
		return s.PutEntry(ctx, in)
	}
	return nil, errors.Errorf("no servers")
}
