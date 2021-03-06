package itest

import (
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

func PutBatch(
	t *testing.T,
	nBatch int,
	maxInFlight int,
	ws ...*wallelib.Writer) time.Duration {
	t0 := time.Now()
	puts := make([]*wallelib.PutCtx, 0, nBatch)
	putT0 := make([]time.Time, 0, nBatch)
	putIdx := 0
	latencies := make([]time.Duration, 0, nBatch)
	for i := 0; i < nBatch; i++ {
		putCtx := ws[i%len(ws)].PutEntry([]byte("testingoooo " + strconv.Itoa(i)))
		puts = append(puts, putCtx)
		putT0 = append(putT0, time.Now())

		ok, l := resolvePutCtx(t, puts[putIdx], putT0[putIdx], i-putIdx > maxInFlight)
		if ok {
			latencies = append(latencies, l)
			putIdx += 1
		}
	}
	for i := putIdx; i < len(puts); i++ {
		_, l := resolvePutCtx(t, puts[i], putT0[putIdx], true)
		latencies = append(latencies, l)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	zlog.Info(
		"TEST: processed all entries: ",
		len(puts), " writers: ", len(ws), " inflight: ", maxInFlight, " ",
		" p95: ", latencies[len(latencies)*95/100],
		" p99: ", latencies[len(latencies)*99/100],
		" p999: ", latencies[len(latencies)*999/1000],
		" total: ", time.Now().Sub(t0),
	)
	return latencies[len(latencies)-1]
}

func resolvePutCtx(
	t *testing.T,
	putCtx *wallelib.PutCtx,
	putT0 time.Time,
	block bool) (bool, time.Duration) {
	if !block {
		select {
		case <-putCtx.Done():
		default:
			return false, 0
		}
	} else {
		timeout := wallelib.ReconnectDelay + 5*time.Second // reconnect delay + put entry timeout
		select {
		case <-putCtx.Done():
		case <-time.After(putT0.Add(timeout).Sub(time.Now())):
			t.Fatalf("timedout waiting on putCtx, entryId: %d", putCtx.Entry.EntryId)
		}
	}
	latency := time.Now().Sub(putT0)
	require.NoError(t, putCtx.Err())
	if putCtx.Entry.EntryId%1000 == 0 {
		zlog.Info("TEST: putEntry success ", putCtx.Entry.EntryId)
	}
	return true, latency
}
