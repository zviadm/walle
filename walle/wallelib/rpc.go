package wallelib

import (
	"github.com/zviadm/walle/proto/walleapi"
)

type Client interface {
	ForStream(streamURI string) walleapi.WalleApiClient
}
