package walle

import (
	"context"

	"google.golang.org/grpc"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
)

type mockSystem struct {
	walle_pb.WalleClient
	servers map[string]*Server
}

func newMockSystem(serverIds []string) (*mockSystem, *mockClient) {
	mSystem := &mockSystem{servers: make(map[string]*Server, len(serverIds))}
	mClient := &mockClient{mSystem}
	for _, serverId := range serverIds {
		m := newMockStorage([]string{"/mock/1"})
		mSystem.servers[serverId] = NewServer(serverId, m, mClient)
	}
	mSystem.servers[""] = mSystem.servers[serverIds[0]]
	return mSystem, mClient
}

func (m *mockSystem) PutEntry(
	ctx context.Context,
	in *walle_pb.PutEntryRequest,
	opts ...grpc.CallOption) (*walle_pb.BaseResponse, error) {
	return m.servers[in.TargetServerId].PutEntry(ctx, in)
}

func (m *mockSystem) NewWriter(
	ctx context.Context,
	in *walle_pb.NewWriterRequest,
	opts ...grpc.CallOption) (*walle_pb.BaseResponse, error) {
	return m.servers[in.TargetServerId].NewWriter(ctx, in)
}

func (m *mockSystem) LastEntry(
	ctx context.Context,
	in *walle_pb.LastEntryRequest,
	opts ...grpc.CallOption) (*walle_pb.LastEntryResponse, error) {
	return m.servers[in.TargetServerId].LastEntry(ctx, in)
}

type mockClient struct {
	m *mockSystem
}

func (m *mockClient) Preferred(streamURI string) walle_pb.WalleClient {
	return m.m
}
func (m *mockClient) ForServer(serverId string) walle_pb.WalleClient {
	return m.m
}
func (m *mockClient) ForServerNoFallback(serverId string) (walle_pb.WalleClient, error) {
	_, ok := m.m.servers[serverId]
	if !ok {
		return nil, errors.Errorf("unknown serverId: %s", serverId)
	}
	return m.m, nil
}
