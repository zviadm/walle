package wallelib

import (
	"os"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
)

const (
	envRootPb = "WALLE_ROOTPB"
)

func RootPbFromEnv() (*walleapi.Topology, error) {
	rootPb := os.Getenv(envRootPb)
	if rootPb == "" {
		return nil, errors.Errorf("Environment variable: %s not defined", envRootPb)
	}
	return TopologyFromFile(rootPb)
}
