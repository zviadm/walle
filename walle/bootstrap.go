package walle

import (
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
)

func BootstrapRoot(s storage.Storage, rootURI string, rootFile string, rootInfo *walleapi.ServerInfo) error {
	var entryId int64 = 1
	rootPb := &walleapi.Topology{
		Version: entryId,
		Streams: map[string]*walleapi.StreamTopology{
			rootURI: &walleapi.StreamTopology{
				Version:   1,
				ServerIds: []string{s.ServerId()},
			},
		},
		Servers: map[string]*walleapi.ServerInfo{s.ServerId(): rootInfo},
	}
	if err := wallelib.TopologyToFile(rootPb, rootFile); err != nil {
		return err
	}
	ss := s.NewStream(rootURI, rootPb.Streams[rootURI])
	entryData, err := rootPb.Marshal()
	if err != nil {
		return err
	}
	entry := &walleapi.Entry{
		EntryId:     entryId,
		WriterId:    storage.Entry0.WriterId,
		ChecksumMd5: wallelib.CalculateChecksumMd5(storage.Entry0.ChecksumMd5, entryData),
		Data:        entryData,
	}
	if err != nil {
		return err
	}
	if !ss.PutEntry(entry, true) {
		return errors.Errorf("couldn't initialize rootURI with initial entry: %s -- %+v", rootURI, entry)
	}
	return nil
}
