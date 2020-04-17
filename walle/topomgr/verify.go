package topomgr

import (
	"github.com/zviadm/walle/proto/walleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Verifies if it is safe to change streamURI memebers to new set of
// serverIds. Returns true if there actually is any change at all, returns
// an error if change is not safe or valid.
func verifyAndDiffMembershipChange(
	t *walleapi.Topology, streamURI string, serverIds []string) (bool, error) {
	if len(serverIds) == 0 {
		return false, status.Errorf(codes.InvalidArgument, "serverIds list can't be empty")
	}
	for _, serverId := range serverIds {
		if _, ok := t.Servers[serverId]; !ok {
			return false, status.Errorf(codes.FailedPrecondition, "invalid serverId: %s", serverId)
		}
	}
	serverIdsDiff := make(map[string]struct{}, len(serverIds)+1)
	for _, serverId := range serverIds {
		serverIdsDiff[serverId] = struct{}{}
	}
	if len(serverIdsDiff) != len(serverIds) {
		return false, status.Errorf(codes.InvalidArgument, "serverIds must be unique: %s", serverIds)
	}

	streamT, ok := t.Streams[streamURI]
	if !ok {
		return true, nil
	}
	for _, serverId := range streamT.ServerIds {
		if _, ok := serverIdsDiff[serverId]; ok {
			delete(serverIdsDiff, serverId)
		} else {
			serverIdsDiff[serverId] = struct{}{}
		}
	}
	if len(serverIdsDiff) > 1 {
		return false, status.Errorf(
			codes.FailedPrecondition,
			"too many changes in serverIds: %s -> %s (diff: %s)",
			streamT.ServerIds, serverIds, serverIdsDiff)
	}
	return len(serverIdsDiff) > 0, nil
}
