package walle

import (
	"context"
	"time"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/zlog"
)

const (
	maxGapBatch = 100000 // Maximum GAP entries processed in one batch.
)

// Gap handler detects and fills gaps in streams in background.
func (s *Server) gapHandler(ctx context.Context) {
	for {
		for _, streamURI := range s.s.LocalStreams() {
			ss, ok := s.s.Stream(streamURI)
			if !ok {
				continue
			}
			for {
				gapStart, gapEnd := ss.GapRange()
				if gapStart >= gapEnd {
					break
				}
				gapEndFinal := gapEnd
				if gapEnd > gapStart+maxGapBatch {
					gapEnd = gapStart + maxGapBatch
				}
				err := s.gapHandlerForStream(ctx, ss, gapStart, gapEnd)
				if err != nil {
					zlog.Warningf("[gh] err filling gap: %s %d -> %d, %s", ss.StreamURI(), gapStart, gapEnd, err)
					break
				}
				zlog.Infof("[gh] filled: %s %d -> %d (end: %d)", ss.StreamURI(), gapStart, gapEnd, gapEndFinal)
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
	// Flushing isn't necessary from correctness perspective, however waiting on flush
	// will throttle Gap filling so that it doesn't write too much data without any flushes
	// happening.
	return s.s.Flush(ctx)
}

// Reads and processes committed entries in range: [startId, endId). Will backfill any of the
// missing entries. [startId, endId), must be a valid committed range.
func (s *Server) readAndProcessEntries(
	ctx context.Context,
	ss storage.Stream,
	startId int64,
	endId int64,
	processEntry func(entry *walleapi.Entry) error) error {
	var cursor storage.Cursor
	defer func() {
		if cursor != nil {
			cursor.Close()
		}
	}()

	entryId := startId
	for entryId < endId {
		if cursor == nil {
			var err error
			cursor, err = ss.ReadFrom(entryId)
			if err != nil {
				return err
			}
		}
		entryIdLocal, ok := cursor.Next()
		if !ok {
			return errors.Errorf(
				"ERR_FATAL; committed entry wasn't found by cursor: %d > %d (from: %d)!",
				entryIdLocal, endId, entryId)
		}
		if entryIdLocal > entryId {
			if entryIdLocal > endId {
				entryIdLocal = endId // This ends processing.
			}
			// cursor.Close()
			// cursor = nil
			err := s.fetchAndStoreEntries(
				ctx, ss, entryId, entryIdLocal, processEntry)
			if err != nil {
				return err
			}
		}
		if processEntry != nil {
			entry := cursor.Entry()
			if err := processEntry(entry); err != nil {
				return err
			}
		}
		entryId = entryIdLocal + 1
	}
	return nil
}

// fetchAndStoreEntries fetches committed entries from other servers in range: [startId, endId), and commits
// them locally. Entries are streamed and stored right away thus, partial success is possible. Returns success
// only if all entries were successfully fetched and stored locally.
func (s *Server) fetchAndStoreEntries(
	ctx context.Context,
	ss storage.Stream,
	startId int64, endId int64,
	processEntry func(entry *walleapi.Entry) error) error {

	ssTopology := ss.Topology()
	var errs []error
	for _, serverId := range ssTopology.ServerIds {
		if serverId == s.s.ServerId() {
			continue
		}

		c, err := s.c.ForServer(serverId)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for {
			streamCtx, cancel := context.WithCancel(ctx)
			entries, readErr := readEntriesAll(streamCtx, c, &walle_pb.ReadEntriesRequest{
				ServerId:      serverId,
				StreamUri:     ss.StreamURI(),
				StreamVersion: ssTopology.Version,
				FromServerId:  s.s.ServerId(),
				StartEntryId:  startId,
				EndEntryId:    endId,
			})
			cancel()
			if len(entries) > 0 {
				err := ss.PutGapEntries(entries)
				if err != nil {
					return err
				}
				for _, entry := range entries {
					if processEntry != nil {
						if err := processEntry(entry); err != nil {
							return err
						}
					}
				}

				startId = entries[len(entries)-1].EntryId + 1
				if startId >= endId {
					return nil
				}
			}
			if readErr != nil {
				errs = append(errs, readErr)
				break
			}
		}
	}
	return errors.Errorf("err fetching: %s - %s", ss.StreamURI(), errs)
}
