package walle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zviadm/walle/proto/walleapi"
)

func TestStorageOpen(t *testing.T) {
	dbPath := storageTmpTestDir()
	s, err := StorageInit(dbPath, true)
	require.NoError(t, err)
	s.NewStream("/s/1", &walleapi.StreamTopology{Version: 1})
	s.Close()

	s, err = StorageInit(dbPath, false)
	require.NoError(t, err)
	defer s.Close()
	require.EqualValues(t, s.Streams(false), []string{"/s/1"})
}
