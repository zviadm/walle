package main

import (
	"context"
	"flag"
	"fmt"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/zviadm/stats-go/exporters/datadog"
	"github.com/zviadm/stats-go/metrics"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

var (
	benchData = make([]byte, wallelib.MaxEntrySize)
)

var (
	totalPutsCounter   = metrics.DefineCounter("wctl/puts", metrics.WithTags("stream_uri"))
	putLatency99Gauge  = metrics.DefineGauge("wctl/put_latency_ms_p99", metrics.WithTags("stream_uri"))
	putLatency999Gauge = metrics.DefineGauge("wctl/put_latency_ms_p999", metrics.WithTags("stream_uri"))
)

var memBallast []byte

func cmdBench(
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string,
	args []string) {
	f := flag.NewFlagSet("cmd.bench", flag.ExitOnError)
	uriPrefix := f.String("prefix", "/bench", "Stream URI prefix for streams to write to.")
	nStreams := f.Int("streams", 1, "Total streams to use for benchmarking. Streams are zero indexed: <prefix>/<idx>")
	writerLease := f.Duration("lease", 10*time.Second, "Writer lease duration.")
	qps := f.Int("qps", 10, "Target total QPS.")
	throughputKBs := f.Int("kbs", 10, "Target total throughput in KB per second.")
	totalTime := f.Duration("time", 0, "Total bench duration. If 0, will run forever.")
	f.Parse(args)

	metrics.SetInstanceName("wctl")
	err := datadog.ExporterGo(ctx)
	exitOnErr(err)

	memBallast = make([]byte, *throughputKBs*1024*5)

	c, err := wallelib.NewClientFromRootPb(ctx, rootPb, clusterURI)
	exitOnErr(err)

	ws := make([]*wallelib.Writer, *nStreams)
	for idx := range ws {
		ws[idx], err = wallelib.WaitAndClaim(
			ctx, c, path.Join(*uriPrefix, strconv.Itoa(idx)), "bench:0000", *writerLease)
		exitOnErr(err)
		defer ws[idx].Close()
	}
	fmt.Println("writers claimed, starting benchmark...")
	if *totalTime > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *totalTime)
		defer cancel()
	}
	for i := range benchData {
		benchData[i] = byte(i)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(ws))
	for idx, w := range ws {
		go func(idx int, w *wallelib.Writer) {
			putBatch(ctx, idx, w, *qps/len(ws), (*throughputKBs)*1024/len(ws))
			wg.Done()
		}(idx, w)
	}
	wg.Wait()
}

func putBatch(
	ctx context.Context,
	wIdx int,
	w *wallelib.Writer,
	qps int,
	tps int) {

	metricsKV := metrics.KV{"stream_uri": w.StreamURI()}
	totalPutsC := totalPutsCounter.V(metricsKV)
	putLatency99G := putLatency99Gauge.V(metricsKV)
	putLatency999G := putLatency999Gauge.V(metricsKV)

	maxInFlight := 10000
	progressN := 1000

	puts := make([]*wallelib.PutCtx, 0, maxInFlight)
	putT0 := make([]time.Time, 0, maxInFlight)
	latencies := make([]time.Duration, 0, maxInFlight)
	putIdx := 0
	putTotalSize := 0

	// TODO(zviad): It would be better if this was random
	dataSize := (tps + qps - 1) / qps
	if dataSize > wallelib.MaxEntrySize {
		dataSize = wallelib.MaxEntrySize
	}
	ticker := time.NewTicker(time.Second / time.Duration(qps))
	defer ticker.Stop()

	t0 := time.Now()
	printProgress := func() {
		tDelta := time.Now().Sub(t0)
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p99 := latencies[len(latencies)*99/100]
		p999 := latencies[len(latencies)*999/1000]
		zlog.Infof(
			"Bench[%d]: processed: %d, (entryId: %d) p50: %s p95: %s p99: %s p999: %s, QPS: %.2f (Target: %d), KB/s: %.1f",
			wIdx, putIdx, puts[putIdx-1].Entry.EntryId,
			latencies[len(latencies)*50/100],
			latencies[len(latencies)*95/100],
			p99, p999,
			float64(putIdx)/tDelta.Seconds(), qps,
			float64(putTotalSize)/1024.0/tDelta.Seconds(),
		)
		totalPutsC.Count(float64(putIdx))
		putLatency99G.Set(p99.Seconds() * 1000.0)
		putLatency999G.Set(p999.Seconds() * 1000.0)
	}
	for i := 0; ctx.Err() == nil; i++ {
		var putDone <-chan struct{}
		if putIdx < len(puts) {
			putDone = puts[putIdx].Done()
		}
		select {
		case <-ticker.C:
			dataIdx := i % (wallelib.MaxEntrySize - dataSize + 1)
			putCtx := w.PutEntry(benchData[dataIdx : dataIdx+dataSize])
			puts = append(puts, putCtx)
			putT0 = append(putT0, time.Now())
		case <-putDone:
		}
		ok, l := resolvePutCtx(puts[putIdx], putT0[putIdx], len(puts) >= maxInFlight)
		if ok {
			latencies = append(latencies, l)
			putTotalSize += len(puts[putIdx].Entry.Data)
			putIdx += 1
		}

		if putIdx == progressN {
			printProgress()
			copy(puts, puts[putIdx:])
			puts = puts[:len(puts)-putIdx]
			copy(putT0, putT0[putIdx:])
			putT0 = putT0[:len(putT0)-putIdx]
			latencies = latencies[:0]
			putIdx = 0
			putTotalSize = 0
			t0 = time.Now()
		}
	}
	for ; putIdx < len(puts); putIdx++ {
		_, l := resolvePutCtx(puts[putIdx], putT0[putIdx], true)
		latencies = append(latencies, l)
		putTotalSize += len(puts[putIdx].Entry.Data)
	}
	printProgress()
	return
}

func resolvePutCtx(
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
		<-putCtx.Done()
	}
	latency := time.Now().Sub(putT0)
	exitOnErr(putCtx.Err())
	return true, latency
}
