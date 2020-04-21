package storage

import (
	"context"
	"encoding/binary"
	"flag"
	"time"

	"github.com/zviadm/stats-go/metrics"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/wt"
)

var flagTrimMaxQPS = flag.Int(
	"walle.storage.trim_max_qps", 50*1000,
	"Limits maximum entries that will be trimmed per second. "+
		"This is useful to avoid latency spikes due to background trimming.")

func (m *storage) trimLoop(ctx context.Context) {
	defer m.backgroundWG.Done()
	s, err := m.c.OpenSession()
	panic.OnErr(err)
	trimQP5ms := *flagTrimMaxQPS/200 + 1
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
			m.trimStreamTo(ctx, s, streamURI, trimTo, trimQP5ms)
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
	trimToEntryId int64,
	trimQP5ms int) {
	metricsKV := metrics.KV{"stream_uri": streamURI}
	trimsC := trimsCounter.V(metricsKV)
	trimTotalMsC := trimTotalMsCounter.V(metricsKV)
	tables := []string{streamDS(streamURI), streamBackfillDS(streamURI)}
	for _, table := range tables {
		c, err := s.OpenCursor(table)
		panic.OnErr(err)
		for ctx.Err() == nil {
			t0 := time.Now()
			trimN := 0
			for ; trimN < trimQP5ms; trimN++ {
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
			trimsC.Count(float64(trimN))
			trimTotalMsC.Count(time.Now().Sub(t0).Seconds() * 1000.0)
			if trimN < trimQP5ms {
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Millisecond):
			}
		}
		panic.OnErr(c.Close())
	}
}
