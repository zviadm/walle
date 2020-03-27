package topomgr

import (
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
)

func verifyServerIds(servers map[string]*walleapi.ServerInfo, serverIds []string) error {
	for _, serverId := range serverIds {
		if _, ok := servers[serverId]; !ok {
			return errors.Errorf("invalid serverId: %s", serverId)
		}
	}
	return nil
}

func verifyMembershipChange(currentServerIds []string, newServerIds []string) (string, error) {
	var serverIdsDiff map[string]struct{}
	for _, serverId := range newServerIds {
		serverIdsDiff[serverId] = struct{}{}
	}
	if len(serverIdsDiff) != len(newServerIds) {
		return "", errors.Errorf("serverIds must be unique: %s", newServerIds)
	}
	for _, serverId := range currentServerIds {
		if _, ok := serverIdsDiff[serverId]; ok {
			delete(serverIdsDiff, serverId)
		} else {
			serverIdsDiff[serverId] = struct{}{}
		}
	}
	if len(serverIdsDiff) > 1 {
		return "", errors.Errorf(
			"too many changes in serverIds: %s -> %s (diff: %s)",
			currentServerIds, newServerIds, serverIdsDiff)
	}
	for serverId := range serverIdsDiff {
		return serverId, nil
	}
	return "", nil
}
