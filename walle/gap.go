package walle

import (
	"context"
	"io"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
)

func (s *Server) gapHandler(ctx context.Context) {
	for {
		for _, streamURI := range s.s.Streams() {
			ss, ok := s.s.Stream(streamURI)
			if !ok {
				continue
			}
			noGapCommittedId, committedId, _ := ss.CommittedEntryIds()
			if noGapCommittedId >= committedId {
				continue
			}
			err := s.gapHandlerForStream(ctx, ss, noGapCommittedId, committedId)
			if err != nil {
				glog.Warningf("[%s] error filling gap: %d -> %d, %s", ss.StreamURI(), noGapCommittedId, committedId, err)
			}
			glog.Infof("[%s] gap filled: %d -> %d", ss.StreamURI(), noGapCommittedId, committedId)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second): // TODO(zviad): Do this based on notifications?
		}
	}
}

func (s *Server) gapHandlerForStream(
	ctx context.Context,
	ss StreamStorage,
	noGapCommittedId int64,
	committedId int64) error {
	cursor := ss.ReadFrom(noGapCommittedId + 1)
	for noGapCommittedId < committedId {
		entry, ok := cursor.Next()
		if !ok || entry.EntryId > committedId {
			panic("DeveloperError; CommittedEntry wasn't found by cursor!")
		}
		if entry.EntryId > noGapCommittedId+1 {
			err := s.fetchAndStoreEntries(ctx, ss, noGapCommittedId+1, entry.EntryId)
			if err != nil {
				return err
			}
		}
		ss.UpdateNoGapCommittedId(entry.EntryId)
		noGapCommittedId, committedId, _ = ss.CommittedEntryIds()
	}
	return nil
}

// Fetches committed entries from other servers in range: [startId, endId), and commit them locally.
// Entries are streamed and stored  right away thus, partial success is possible. Returns success only if
// all entries were successfully fetched and stored locally.
func (s *Server) fetchAndStoreEntries(
	ctx context.Context,
	ss StreamStorage,
	startId int64, endId int64) error {

	ssTopology := ss.Topology()
Main:
	for _, serverId := range ssTopology.ServerIds {
		if serverId == s.serverId {
			continue
		}

		c, err := s.c.ForServer(serverId)
		if err != nil {
			glog.Warningf("[%s] failed to connect to: %s for fetching entries: %s", ss.StreamURI(), serverId, err)
			continue
		}
		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		r, err := c.ReadEntries(streamCtx, &walle_pb.ReadEntriesRequest{
			ServerId:      serverId,
			StreamUri:     ss.StreamURI(),
			StreamVersion: ssTopology.Version,
			StartEntryId:  startId,
			EndEntryId:    endId,
		})
		if err != nil {
			glog.Warningf("[%s] failed to establish stream to: %s for fetching entries: %s", ss.StreamURI(), serverId, err)
			continue
		}
		for {
			entry, err := r.Recv()
			if err != nil {
				if err == io.EOF {
					if startId != endId {
						glog.Errorf("[%s] DEVELOPER_ERROR; unreachable code. server: %s is buggy!", ss.StreamURI(), serverId)
						continue Main
					}
					return nil
				}
				glog.Warningf("[%s] failed to fetch all entries from: %s, %s", ss.StreamURI(), serverId, err)
				continue Main
			}
			if entry.EntryId != startId {
				glog.Errorf("[%s] DEVELOPER_ERROR; unreachable code. server: %s is buggy!", ss.StreamURI(), serverId)
				continue Main
			}
			startId += 1
			ok := ss.PutEntry(entry, true)
			if !ok {
				panic("DeveloperError; Putting CommittedEntry must always succeed!")
			}
		}
	}
	return errors.Errorf("[%s] couldn't fetch all entries, tried all servers...", ss.StreamURI())
}
