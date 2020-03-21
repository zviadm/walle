package walle

import (
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/wallelib"
)

func BootstrapRoot(s Storage, rootURI string, rootTopoFile string, addr string) error {
	var entryId int64 = 1
	topology := &walleapi.Topology{
		Version: entryId,
		Streams: map[string]*walleapi.StreamTopology{
			rootURI: &walleapi.StreamTopology{
				Version:   entryId,
				ServerIds: []string{s.ServerId()},
			},
		},
		Servers: map[string]string{s.ServerId(): addr},
	}
	if err := wallelib.TopologyToFile(topology, rootTopoFile); err != nil {
		return err
	}
	s.NewStream(rootURI, topology.Streams[rootURI])
	ss, ok := s.Stream(rootURI, true)
	if !ok {
		return errors.Errorf("stream was just created, must exist for: %s", rootURI)
	}
	entryData, err := topology.Marshal()
	if err != nil {
		return err
	}
	entry := &walleapi.Entry{
		EntryId:     entryId,
		ChecksumMd5: wallelib.CalculateChecksumMd5(entry0.ChecksumMd5, entryData),
		Data:        entryData,
	}
	if !ss.PutEntry(entry, true) {
		return errors.Errorf("couldn't initialize rootURI with initial entry: %s -- %+v", rootURI, entry)
	}
	return nil
}
