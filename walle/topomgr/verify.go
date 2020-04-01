package topomgr

import (
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
)

// Verifies if it is safe to change streamURI memebers to new set of
// serverIds. Returns true if there actually is any change at all, returns
// an error if change is not safe or valid.
func verifyAndDiffMembershipChange(
	t *walleapi.Topology, streamURI string, serverIds []string) (bool, error) {
	for _, serverId := range serverIds {
		if _, ok := t.Servers[serverId]; !ok {
			return false, errors.Errorf("invalid serverId: %s", serverId)
		}
	}

	serverIdsDiff := make(map[string]struct{}, len(serverIds)+1)
	for _, serverId := range serverIds {
		serverIdsDiff[serverId] = struct{}{}
	}
	if len(serverIdsDiff) != len(serverIds) {
		return false, errors.Errorf("serverIds must be unique: %s", serverIds)
	}

	streamT, ok := t.Streams[streamURI]
	if !ok {
		return len(serverIds) != 0, nil
	}
	for _, serverId := range streamT.ServerIds {
		if _, ok := serverIdsDiff[serverId]; ok {
			delete(serverIdsDiff, serverId)
		} else {
			serverIdsDiff[serverId] = struct{}{}
		}
	}
	if len(serverIdsDiff) > 1 {
		return false, errors.Errorf(
			"too many changes in serverIds: %s -> %s (diff: %s)",
			streamT.ServerIds, serverIds, serverIdsDiff)
	}
	return len(serverIdsDiff) > 0, nil
}
