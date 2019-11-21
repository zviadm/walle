package walle

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	walle_pb "github.com/zviadm/walle/proto/walle"
)

type mockSystem struct {
	walle_pb.WalleClient
	servers map[string]*Server
}

func newMockSystem(serverIds []string) *mockSystem {
	servers := make(map[string]*Server, len(serverIds))
	for _, serverId := range serverIds {
		m := newMockStorage([]string{"/mock/1"})
		servers[serverId] = NewServer(serverId, m)
	}
	return &mockSystem{servers: servers}
}

func (m *mockSystem) PutEntry(
	ctx context.Context,
	in *walle_pb.PutEntryRequest,
	opts ...grpc.CallOption) (*walle_pb.BaseResponse, error) {

	// TODO(zviad): This forwarding logic will move directly inside 'Server' object.
	if in.TargetServerId != "" {
		return m.servers[in.TargetServerId].PutEntry(ctx, in)
	} else {
		var successIds []string
		var errs []error
		for serverId, s := range m.servers {
			in.TargetServerId = serverId
			_, err := s.PutEntry(ctx, in)
			if err != nil {
				errs = append(errs, err)
			} else {
				successIds = append(successIds, serverId)
			}
		}
		if len(successIds) <= len(errs) {
			return nil, errors.Errorf("not enough success: %s vs fails: %d\nerrs: %v", successIds, len(errs), errs)
		}
		return &walle_pb.BaseResponse{SuccessIds: successIds, Fails: int32(len(errs))}, nil
	}
}

func (m *mockSystem) NewWriter(
	ctx context.Context,
	in *walle_pb.NewWriterRequest,
	opts ...grpc.CallOption) (*walle_pb.BaseResponse, error) {

	// TODO(zviad): This forwarding logic will move directly inside 'Server' object.
	if in.TargetServerId != "" {
		return m.servers[in.TargetServerId].NewWriter(ctx, in)
	} else {
		var successIds []string
		var errs []error
		for serverId, s := range m.servers {
			in.TargetServerId = serverId
			_, err := s.NewWriter(ctx, in)
			if err != nil {
				errs = append(errs, err)
			} else {
				successIds = append(successIds, serverId)
			}
		}
		if len(successIds) <= len(errs) {
			return nil, errors.Errorf("not enough success: %s vs errs: %d\nerrs: %v", successIds, len(errs), errs)
		}
		return &walle_pb.BaseResponse{SuccessIds: successIds, Fails: int32(len(errs))}, nil
	}
}

func (m *mockSystem) LastEntry(
	ctx context.Context,
	in *walle_pb.LastEntryRequest,
	opts ...grpc.CallOption) (*walle_pb.LastEntryResponse, error) {
	return m.servers[in.TargetServerId].LastEntry(ctx, in)
}
