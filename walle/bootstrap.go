package walle

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
)

// BootstrapRoot creates storage with rootURI stream and populates stream with
// first topology entry for given rootURI and root server information.
func BootstrapRoot(s storage.Storage, rootURI string, rootFile string, rootInfo *walleapi.ServerInfo) error {
	if err := storage.ValidateStreamURI(rootURI); err != nil {
		return err
	}
	if !strings.HasPrefix(rootURI, topomgr.Prefix) {
		return errors.Errorf("root_uri must have: %s prefix", topomgr.Prefix)
	}

	var entryId int64 = 1
	rootPb := &walleapi.Topology{
		RootUri: rootURI,
		Version: entryId,
		Streams: map[string]*walleapi.StreamTopology{
			rootURI: {
				Version:   1,
				ServerIds: []string{s.ServerId()},
			},
		},
		Servers: map[string]*walleapi.ServerInfo{s.ServerId(): rootInfo},
	}
	if err := wallelib.TopologyToFile(rootPb, rootFile); err != nil {
		return err
	}
	err := s.Update(rootURI, rootPb.Streams[rootURI])
	if err != nil {
		return err
	}
	entryData, err := rootPb.Marshal()
	if err != nil {
		return err
	}
	entry := &walleapi.Entry{
		EntryId:    entryId,
		WriterId:   storage.Entry0.WriterId,
		ChecksumXX: wallelib.CalculateChecksumXX(storage.Entry0.ChecksumXX, entryData),
		Data:       entryData,
	}
	if err != nil {
		return err
	}
	ss, _ := s.Stream(rootURI)
	return ss.PutEntry(entry, true)
}
