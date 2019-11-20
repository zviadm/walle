package wallelib

import (
	"bytes"
	"context"
	"crypto/md5"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"google.golang.org/grpc"
)

type serverInfo struct {
	mx          sync.Mutex
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
	entry := proto.Clone(in.Entry).(*walle_pb.Entry)
	if in.TargetServerId != "" {
		err := m.putEntry(in.TargetServerId, entry, in.CommittedEntryId)
		if err != nil {
			return nil, err
		}
		return &walle_pb.BaseResponse{SuccessIds: []string{in.TargetServerId}}, nil
	} else {
		var successIds []string
		var fails int32
		for serverId := range m.servers {
			err := m.putEntry(serverId, entry, in.CommittedEntryId)
			if err != nil {
				fails += 1
			} else {
				successIds = append(successIds, serverId)
			}
		}
		if int32(len(successIds)) <= fails {
			return nil, errors.Errorf("not enough success: %s vs fails: %d", successIds, fails)
		}
		return &walle_pb.BaseResponse{SuccessIds: successIds, Fails: fails}, nil
	}
}

func (m *mockSystem) putEntry(serverId string, entry *walle_pb.Entry, committed int64) error {
	sInfo, ok := m.servers[serverId]
	if !ok {
		return errors.Errorf("serverId not found: %s", serverId)
	}
	sInfo.mx.Lock()
	defer sInfo.mx.Unlock()
	if sInfo.maxWriterId > entry.WriterId {
		return errors.Errorf("writerId too small: %s > %s", sInfo.maxWriterId, entry.WriterId)
	}
	sInfo.maxWriterId = entry.WriterId
	if entry.EntryId > sInfo.committed {
		if int64(len(sInfo.entries)) < entry.EntryId {
			return errors.Errorf("entries missing: %d > %d", entry.EntryId, len(sInfo.entries))
		}
		prevEntry := sInfo.entries[entry.EntryId-1]
		verifyMd5 := calculateChecksumMd5(prevEntry.ChecksumMd5, entry.Data)
		if bytes.Compare(verifyMd5, entry.ChecksumMd5) != 0 {
			return errors.Errorf("checksum md5 mismatch: %s != %s", verifyMd5, entry.ChecksumMd5)
		}

		if int64(len(sInfo.entries)) > entry.EntryId {
			if sInfo.entries[len(sInfo.entries)-1].WriterId == entry.WriterId {
				return nil
			}
			sInfo.entries = sInfo.entries[:entry.EntryId-1]
		}
		sInfo.entries = append(sInfo.entries, entry)
	}
	if committed > sInfo.committed {
		if committed > int64(len(sInfo.entries)) {
			return errors.Errorf("mockSystem doesn't support gaps or refills: %d > %d", committed, len(sInfo.entries))
		}
		sInfo.committed = committed
	}
	return nil
}
