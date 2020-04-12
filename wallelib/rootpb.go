package wallelib

import (
	"os"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
)

const (
	// EnvRootPb is environment variable that holds path to WALLE root.pb file.
	EnvRootPb = "WALLE_ROOTPB"
)

// RootPbFromEnv reads and parses root.pb file that EnvRootPb points to.
func RootPbFromEnv() (*walleapi.Topology, error) {
	rootPb := os.Getenv(EnvRootPb)
	if rootPb == "" {
		return nil, errors.Errorf("Environment variable: %s not defined", EnvRootPb)
	}
	return TopologyFromFile(rootPb)
}
