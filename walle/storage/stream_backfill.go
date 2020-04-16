package storage

import (
	"encoding/binary"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *streamStorage) PutGapEntry(entry *walleapi.Entry) error {
	m.backfillMX.Lock()
	defer m.backfillMX.Unlock()
	if m.sessFill.Closed() {
		return status.Errorf(codes.NotFound, "%s not found", m.streamURI)
	}
	t0 := time.Now()
	binary.BigEndian.PutUint64(m.backfillBuf8, uint64(entry.EntryId))
	n, err := entry.MarshalTo(m.backfillEntryBuf)
	panic.OnErr(err)
	panic.OnErr(m.streamFillW.Insert(m.backfillBuf8, m.backfillEntryBuf[:n]))
	m.backfillsC.Count(1)
	m.backfillBytesC.Count(float64(n))
	m.backfillTotalMsC.Count(time.Now().Sub(t0).Seconds() * 1000.0)
	return nil
}
