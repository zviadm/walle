package walle

import (
	"context"
	"encoding/hex"
	"io"
	"time"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

// Gap handler detects and fills gaps in streams in background.
func (s *Server) gapHandler(ctx context.Context) {
	for {
		for _, streamURI := range s.s.Streams(true) {
			ss, ok := s.s.Stream(streamURI, true)
			if !ok {
				continue
			}
			noGapCommittedId, committedId, _ := ss.CommittedEntryIds()
			if noGapCommittedId >= committedId {
				continue
			}
			err := s.gapHandlerForStream(ctx, ss, noGapCommittedId, committedId)
			if err != nil {
				zlog.Warningf("[gh] err filling gap: %s %d -> %d, %s", ss.StreamURI(), noGapCommittedId, committedId, err)
			}
			zlog.Infof("[gh] filled: %s %d -> %d", ss.StreamURI(), noGapCommittedId, committedId)
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
	defer cursor.Close()
	for noGapCommittedId < committedId {
		entry, ok := cursor.Next()
		if !ok || entry.EntryId > committedId {
			panic("DeveloperError; CommittedEntry wasn't found by cursor!")
		}
		if entry.EntryId > noGapCommittedId+1 {
			err := s.fetchAndStoreEntries(ctx, ss, noGapCommittedId+1, entry.EntryId, nil)
			if err != nil {
				return err
			}
		}
		ss.UpdateNoGapCommittedId(entry.EntryId)
		noGapCommittedId, committedId, _ = ss.CommittedEntryIds()
	}
	return nil
}

// Fetches committed entries from other servers in range: [startId, endId), and commits them locally.
// Entries are streamed and stored right away thus, partial success is possible. Returns success only if
// all entries were successfully fetched and stored locally.
func (s *Server) fetchAndStoreEntries(
	ctx context.Context,
	ss StreamStorage,
	startId int64, endId int64,
	processEntry func(entry *walleapi.Entry) error) error {

	ssTopology := ss.Topology()
Main:
	for _, serverId := range ssTopology.ServerIds {
		if serverId == s.s.ServerId() {
			continue
		}
		serverIdHex := hex.EncodeToString([]byte(serverId))

		c, err := s.c.ForServer(serverId)
		if err != nil {
			if err != wallelib.ErrConnUnavailable {
				zlog.Warningf(
					"[gh] err connecting for %s, to: %s - %s",
					ss.StreamURI(), serverIdHex, err)
			}
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
			zlog.Warningf(
				"[gh] err establishing stream for %s, to: %s - %s",
				ss.StreamURI(), serverIdHex, err)
			continue
		}
		for {
			entry, err := r.Recv()
			if err != nil {
				if err == io.EOF {
					if startId != endId {
						zlog.Errorf(
							"[gh] DEVELOPER_ERROR; unreachable code. %s server: %s is buggy!", ss.StreamURI(), serverIdHex)
						continue Main
					}
					return nil
				}
				zlog.Warningf("[gh] err fetching entries for: %s from: %s - %s", ss.StreamURI(), serverIdHex, err)
				continue Main
			}
			if entry.EntryId != startId {
				zlog.Errorf("[gh] DEVELOPER_ERROR; unreachable code. %s server: %s is buggy!", ss.StreamURI(), serverIdHex)
				continue Main
			}
			startId += 1
			ok := ss.PutEntry(entry, true)
			panicOnNotOk(ok, "putting committed entry must always succeed!")
			if processEntry != nil {
				if err := processEntry(entry); err != nil {
					return err
				}
			}
		}
	}
	return errors.Errorf("couldn't fetch all entries, tried all servers for: %s", ss.StreamURI())
}
