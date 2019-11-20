package wallelib

import (
	"context"
	"crypto/md5"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"google.golang.org/grpc"
)

type serverInfo struct {
	entries     []*walle_pb.Entry
	committed   int64
	maxWriterId string
}

type mockSystem struct {
	walle_pb.WalleClient
	servers map[string]*serverInfo
}

func newMockSystem(serverIds []string) *mockSystem {
	servers := make(map[string]*serverInfo, len(serverIds))
	for _, serverId := range serverIds {
		servers[serverId] = &serverInfo{
			entries: []*walle_pb.Entry{&walle_pb.Entry{ChecksumMd5: make([]byte, md5.Size)}},
		}
	}
	return &mockSystem{servers: servers}
}

func (m *mockSystem) PutEntry(
	ctx context.Context,
	in *walle_pb.PutEntryRequest,
	opts ...grpc.CallOption) (*walle_pb.BaseResponse, error) {
	if in.TargetServerId != "" {
		err := m.putEntry(in.TargetServerId, in.Entry, in.CommittedEntryId)
		if err != nil {
			return nil, err
		}
	}
}

func (m *mockSystem) putEntry(serverId string) error
