package walle

import (
	"context"
	"encoding/hex"
	"io"
	"time"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/walle/walle/storage"
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
				continue
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
	ss storage.Stream,
	noGapCommittedId int64,
	committedId int64) error {
	newGapId := noGapCommittedId
	processEntry := func(e *walleapi.Entry) error {
		newGapId = e.EntryId
		return nil
	}
	err := s.readAndProcessEntries(
		ctx, ss, noGapCommittedId, committedId, processEntry)
	if newGapId > noGapCommittedId {
		ss.UpdateNoGapCommittedId(newGapId)
	}
	return err
}

// Reads and processes committed entries in range: [startId, endId). Will backfill any of the
// missing entries. [startId, endId), must be a valid committed range.
func (s *Server) readAndProcessEntries(
	ctx context.Context,
	ss storage.Stream,
	startId int64,
	endId int64,
	processEntry func(entry *walleapi.Entry) error) error {
	entryId := startId
	cursor := ss.ReadFrom(entryId)
	defer cursor.Close()
	for entryId < endId {
		entry, ok := cursor.Next()
		if !ok || entry.GetEntryId() > endId {
			return errors.Errorf(
				"ERR_FATAL; committed entry wasn't found by cursor: %d > %d (from: %d)!",
				entry.GetEntryId(), endId, entryId)
		}
		if entry.EntryId > entryId {
			err := s.fetchAndStoreEntries(
				ctx, ss, entryId, entry.EntryId, processEntry)
			if err != nil {
				return err
			}
		}
		if processEntry != nil {
			if err := processEntry(entry); err != nil {
				return err
			}
		}
		entryId = entry.EntryId
	}
	return nil
}

// Fetches committed entries from other servers in range: [startId, endId), and commits them locally.
// Entries are streamed and stored right away thus, partial success is possible. Returns success only if
// all entries were successfully fetched and stored locally.
func (s *Server) fetchAndStoreEntries(
	ctx context.Context,
	ss storage.Stream,
	startId int64, endId int64,
	processEntry func(entry *walleapi.Entry) error) error {

	ssTopology := ss.Topology()
	var errs []error
Main:
	for _, serverId := range ssTopology.ServerIds {
		if serverId == s.s.ServerId() {
			continue
		}
		serverIdHex := hex.EncodeToString([]byte(serverId))

		c, err := s.c.ForServer(serverId)
		if err != nil {
			if err != wallelib.ErrConnUnavailable {
				errs = append(errs, err)
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
			errs = append(errs, err)
			continue
		}
		for {
			entry, err := r.Recv()
			if err != nil {
				if err == io.EOF {
					if startId != endId {
						zlog.Errorf(
							"ERR_FATAL; unreachable code. %s server: %s is buggy!",
							ss.StreamURI(), serverIdHex)
						continue Main
					}
					return nil
				}
				errs = append(errs, err)
				continue Main
			}
			if entry.EntryId != startId {
				zlog.Errorf(
					"ERR_FATAL; unreachable code. %s server: %s is buggy!",
					ss.StreamURI(), serverIdHex)
				continue Main
			}
			startId += 1
			ok := ss.PutEntry(entry, true)
			panic.OnNotOk(ok, "putting committed entry must always succeed!")
			if processEntry != nil {
				if err := processEntry(entry); err != nil {
					return err
				}
			}
		}
	}
	return errors.Errorf("err fetching: %s - %s", ss.StreamURI(), errs)
}
