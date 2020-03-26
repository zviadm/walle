package walle

import (
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
)

func BootstrapRoot(s Storage, rootURI string, rootFile string, rootInfo *walleapi.ServerInfo) error {
	var entryId int64 = 1
	rootPb := &walleapi.Topology{
		Version: entryId,
		Streams: map[string]*walleapi.StreamTopology{
			rootURI: &walleapi.StreamTopology{
				Version:   entryId,
				ServerIds: []string{s.ServerId()},
			},
		},
		Servers: map[string]*walleapi.ServerInfo{s.ServerId(): rootInfo},
	}
	if err := wallelib.TopologyToFile(rootPb, rootFile); err != nil {
		return err
	}
	s.NewStream(rootURI, rootPb.Streams[rootURI])
	ss, ok := s.Stream(rootURI, true)
	if !ok {
		return errors.Errorf("stream was just created, must exist for: %s", rootURI)
	}
	entryData, err := rootPb.Marshal()
	if err != nil {
		return err
	}
	entry := &walleapi.Entry{
		EntryId:     entryId,
		WriterId:    entry0.WriterId,
		ChecksumMd5: wallelib.CalculateChecksumMd5(entry0.ChecksumMd5, entryData),
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
