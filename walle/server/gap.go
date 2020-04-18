package server

import (
	"context"
	"io"
	"math/rand"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// maxGapBatch limits maximum number of entries that get processed in single
	// backfilling batch. Processing of a single batch shouldn't take more than few
	// seconds.
	maxGapBatch = 100000
)

// notifyGap notifies backfiller that a gap might have been created for given
// streamURI.
func (s *Server) notifyGap(streamURI string) {
	s.mxGap.Lock()
	s.streamsWithGap[streamURI] = struct{}{}
	s.mxGap.Unlock()
	select {
	case s.notifyGapC <- struct{}{}:
	default:
	}
}

func (s *Server) consumeGapNotifies() map[string]struct{} {
	s.mxGap.Lock()
	defer s.mxGap.Unlock()
	r := s.streamsWithGap
	if len(r) > 0 {
		s.streamsWithGap = make(map[string]struct{}, len(r))
	}
	return r
}

// backfillGapsLoop watches for gaps and backfills them in background.
// This is the only Go routine that makes PutGapEntry calls on storage, thus
// it always makes them in monotonically increasing order.
func (s *Server) backfillGapsLoop(ctx context.Context) {
	for {
		select {
		case <-s.notifyGapC:
		case <-ctx.Done():
			return
		}
		streamsWithGap := s.consumeGapNotifies()
		for len(streamsWithGap) > 0 {
			errAll := true
			for streamURI := range streamsWithGap {
				err := s.checkAndBackfillGap(ctx, streamURI)
				errAll = errAll && (err != nil)
			}
			streamsWithGapNew := s.consumeGapNotifies()
			for streamURI := range streamsWithGapNew {
				streamsWithGap[streamURI] = struct{}{}
			}
			if errAll && len(streamsWithGapNew) == 0 {
				// If everything errored out, take a little
				// break before retrying again.
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
		}
	}
}

func (s *Server) checkAndBackfillGap(ctx context.Context, streamURI string) error {
	ss, ok := s.s.Stream(streamURI)
	if !ok {
		return nil
	}
	for {
		gapStart, gapEnd := ss.GapRange()
		if gapStart >= gapEnd {
			return nil
		}
		gapEndFinal := gapEnd
		if gapEnd > gapStart+maxGapBatch {
			gapEnd = gapStart + maxGapBatch
		}
		err := s.backfillGap(ctx, ss, gapStart, gapEnd)
		if err != nil {
			zlog.Warningf("[gh] err filling gap: %s %d -> %d, %s", ss.StreamURI(), gapStart, gapEnd, err)
			return err
		}
		zlog.Infof("[gh] filled: %s %d -> %d (end: %d)", ss.StreamURI(), gapStart, gapEnd, gapEndFinal)
	}
}

func (s *Server) backfillGap(
	ctx context.Context,
	ss storage.Stream,
	gapStart int64,
	gapEnd int64) error {
	err := s.readAndProcessEntries(
		ctx, ss, gapStart, gapEnd, ss.PutGapEntry, true)
	if err != nil {
		return err
	}
	ss.UpdateGapStart(gapEnd)
	// Flushing isn't necessary from correctness perspective, however waiting on flush
	// will throttle Gap filling so that it doesn't write too much data without any flushes
	// happening.
	return s.s.Flush(ctx)
}

// readAndProcessEntries reads entries in range: [startId, endId) and calls
// processEntry call back on them. Will fetch missing entries from other servers.
// If processFetchedOnly is true, will only run processEntry function on entries that
// were fetched from other servers.
func (s *Server) readAndProcessEntries(
	ctx context.Context,
	ss storage.Stream,
	startId int64,
	endId int64,
	processEntry func(entry *walleapi.Entry) error,
	processFetchedOnly bool) error {
	cursor, err := ss.ReadFrom(startId)
	if err != nil {
		return err
	}
	defer cursor.Close()
	entryId := startId
	for entryId < endId {
		entryIdLocal, ok := cursor.Next()
		if !ok {
			return status.Errorf(
				codes.Internal, "committed entry wasn't found by cursor: %d > %d (from: %d)!",
				entryIdLocal, endId, entryId)
		}
		if entryIdLocal > entryId {
			if entryIdLocal > endId {
				entryIdLocal = endId
			}
			err := s.streamAndProcessEntries(
				ctx, ss, entryId, entryIdLocal, processEntry)
			if err != nil {
				return err
			}
		}
		if entryIdLocal < endId && !processFetchedOnly {
			entry := cursor.Entry()
			if err := processEntry(entry); err != nil {
				return err
			}
		}
		entryId = entryIdLocal + 1
	}
	return nil
}

// streamAndProcessEntries streams committed entries from other servers in range: [startId, endId), and
// calls processEntry callback on them.
func (s *Server) streamAndProcessEntries(
	ctx context.Context,
	ss storage.Stream,
	startId int64, endId int64,
	processEntry func(entry *walleapi.Entry) error) error {

	ssTopology := ss.Topology()
	var errs []error
	for _, idx := range rand.Perm(len(ssTopology.ServerIds)) {
		serverId := ssTopology.ServerIds[idx]
		if serverId == s.s.ServerId() {
			continue
		}

		c, err := s.c.ForServer(serverId)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		streamCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel() // it's ok if this cancel gets delayed till for loop finishes.
		r, err := c.ReadEntries(streamCtx, &walle_pb.ReadEntriesRequest{
			ServerId:      serverId,
			StreamUri:     ss.StreamURI(),
			StreamVersion: ssTopology.Version,
			FromServerId:  s.s.ServerId(),
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
					return nil
				}
				errs = append(errs, err)
				break
			}
			if err := processEntry(entry); err != nil {
				return err
			}
		}
	}
	errCode := codes.Unavailable
	for _, err := range errs {
		if code := status.Convert(err).Code(); wallelib.IsErrFinal(code) {
			errCode = code
		}
	}
	return status.Errorf(errCode, "err fetching: %s - %s", ss.StreamURI(), errs)
}
