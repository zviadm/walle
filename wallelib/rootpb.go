package wallelib

import (
	"os"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
)

const (
	EnvRootPb = "WALLE_ROOTPB"
)

func RootPbFromEnv() (*walleapi.Topology, error) {
	rootPb := os.Getenv(EnvRootPb)
	if rootPb == "" {
		return nil, errors.Errorf("Environment variable: %s not defined", EnvRootPb)
	}
	return TopologyFromFile(rootPb)
}
