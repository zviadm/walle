package wallelib

import (
	walle_pb "github.com/zviadm/walle/proto/walle"
)

type Client interface {
	Preferred(streamURI string) walle_pb.WalleClient
	ForServer(serverId string) walle_pb.WalleClient
	ForServerNoFallback(serverId string) (walle_pb.WalleClient, error)
}
