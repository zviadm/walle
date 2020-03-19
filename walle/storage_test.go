package walle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zviadm/walle/proto/walleapi"
)

func TestSimpleOpen(t *testing.T) {
	s, err := StorageInit(storageTmpTestDir(), true)
	require.NoError(t, err)

	s.NewStream("/s/1", &walleapi.StreamTopology{Version: 1})
}
