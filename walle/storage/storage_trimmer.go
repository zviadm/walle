package storage

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/wt"
)

func (m *storage) trimLoop(ctx context.Context) {
	defer m.backgroundWG.Done()
	s, err := m.c.OpenSession()
	panic.OnErr(err)
	for {
		for _, streamURI := range m.LocalStreams() {
			ss, ok := m.Stream(streamURI)
			if !ok {
				continue
			}
			trimTo := ss.Topology().FirstEntryId
			committed := ss.CommittedId()
			if committed < trimTo {
				trimTo = committed
			}
			m.trimStreamTo(ctx, s, streamURI, trimTo)
		}
		select {
		case <-ctx.Done():
			return
		case <-m.trimQ:
		case <-time.After(5 * time.Minute): // TODO(zviadm): Do this better.
		}
	}
}

func (m *storage) trimStreamTo(
	ctx context.Context,
	s *wt.Session,
	streamURI string,
	trimToEntryId int64) {
	tables := []string{streamDS(streamURI), streamBackfillDS(streamURI)}
	for _, table := range tables {
		c, err := s.OpenCursor(table)
		panic.OnErr(err)
		for ctx.Err() == nil {
			err = c.Next()
			if wt.ErrCode(err) == wt.ErrNotFound {
				break
			}
			panic.OnErr(err)
			unsafeK, err := c.UnsafeKey()
			panic.OnErr(err)
			entryId := int64(binary.BigEndian.Uint64(unsafeK))
			if entryId >= trimToEntryId {
				break
			}
			panic.OnErr(c.Remove())
		}
		panic.OnErr(c.Close())
	}
}
