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
			ss, ok := s.s.Stream(streamURI)
			if !ok {
				continue
			}
			for {
				gapStart, gapEnd := ss.GapRange()
				if gapStart >= gapEnd {
					break
				}
				err := s.gapHandlerForStream(ctx, ss, gapStart, gapEnd)
				if err != nil {
					zlog.Warningf("[gh] err filling gap: %s %d -> %d, %s", ss.StreamURI(), gapStart, gapEnd, err)
					break
				}
				zlog.Infof("[gh] filled: %s %d -> %d", ss.StreamURI(), gapStart, gapEnd)
			}
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
	gapStart int64,
	gapEnd int64) error {
	err := s.readAndProcessEntries(
		ctx, ss, gapStart, gapEnd, nil)
	if err != nil {
		return err
	}
	ss.UpdateGapStart(gapEnd)
	return nil
}

// Reads and processes committed entries in range: [startId, endId). Will backfill any of the
// missing entries. [startId, endId), must be a valid committed range.
func (s *Server) readAndProcessEntries(
	ctx context.Context,
	ss storage.Stream,
	startId int64,
	endId int64,
	processEntry func(entry *walleapi.Entry) error) error {
	cursor, err := ss.ReadFrom(startId)
	if err != nil {
		return err
	}
	defer cursor.Close()

	entryId := startId
	for entryId < endId {
		var ok bool
		var nextEntryId int64
		var nextEntry *walleapi.Entry
		if processEntry != nil {
			nextEntry, ok = cursor.Next()
			nextEntryId = nextEntry.GetEntryId()
		} else {
			nextEntryId, ok = cursor.Skip()
		}
		if !ok || nextEntryId > endId {
			return errors.Errorf(
				"ERR_FATAL; committed entry wasn't found by cursor: %d > %d (from: %d)!",
				nextEntryId, endId, entryId)
		}
		if nextEntryId > entryId+1 {
			err := s.fetchAndStoreEntries(
				ctx, ss, entryId, nextEntryId, processEntry)
			if err != nil {
				return err
			}
		}
		if processEntry != nil {
			if err := processEntry(nextEntry); err != nil {
				return err
			}
		}
		entryId = nextEntryId
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
