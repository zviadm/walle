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

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
)

func cmdBench(
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string,
	args []string) {
	f := flag.NewFlagSet("cmd.bench", flag.ExitOnError)
	uriPrefix := f.String("prefix", "/bench", "Stream URI prefix for streams to write to.")
	nStreams := f.Int("streams", 1, "Total streams to use for benchmarking. Streams are zero indexed: <prefix>/<idx>")
	qps := f.Int("qps", 10, "Target QPS for each stream.")
	totalTime := f.Duration("time", 0, "Total bench duration. If 0, will run forever.")
	f.Parse(args)

	c, err := wallelib.NewClientFromRootPb(ctx, rootPb, clusterURI)
	exitOnErr(err)

	ws := make([]*wallelib.Writer, *nStreams)
	for idx := range ws {
		ws[idx], err = wallelib.WaitAndClaim(
			ctx, c, path.Join(*uriPrefix, strconv.Itoa(idx)), "bench:0000", time.Second)
		exitOnErr(err)
		defer ws[idx].Close()
	}
	fmt.Println("writers claimed, starting benchmark...")
	if *totalTime > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *totalTime)
		defer cancel()
	}
	wg := sync.WaitGroup{}
	wg.Add(len(ws))
	for idx, w := range ws {
		go func(idx int, w *wallelib.Writer) {
			putBatch(ctx, idx, w, *qps)
			wg.Done()
		}(idx, w)
	}
	wg.Wait()
}

func putBatch(
	ctx context.Context,
	wIdx int,
	w *wallelib.Writer,
	qps int) {

	maxInFlight := 10000
	progressN := 1000
	puts := make([]*wallelib.PutCtx, 0, maxInFlight)
	putT0 := make([]time.Time, 0, maxInFlight)
	latencies := make([]time.Duration, 0, maxInFlight)
	putIdx := 0
	ticker := time.NewTicker(time.Second / time.Duration(qps))
	defer ticker.Stop()

	t0 := time.Now()
	printProgress := func() {
		tDelta := time.Now().Sub(t0)
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		zlog.Infof(
			"Bench[%d]: processed: %d, (entryId: %d) p50: %s p95: %s p99: %s p999: %s, QPS: %.2f (Target: %d)",
			wIdx, putIdx, puts[putIdx-1].Entry.EntryId,
			latencies[len(latencies)*50/100],
			latencies[len(latencies)*95/100],
			latencies[len(latencies)*99/100],
			latencies[len(latencies)*999/1000],
			float64(putIdx)/tDelta.Seconds(), qps,
		)
	}
	for i := 0; ctx.Err() == nil; i++ {
		var putDone <-chan struct{}
		if putIdx < len(puts) {
			putDone = puts[putIdx].Done()
		}
		select {
		case <-ticker.C:
			putCtx := w.PutEntry([]byte("testingoooo " + strconv.Itoa(i)))
			puts = append(puts, putCtx)
			putT0 = append(putT0, time.Now())
		case <-putDone:
		}
		ok, l := resolvePutCtx(puts[putIdx], putT0[putIdx], len(puts) >= maxInFlight)
		if ok {
			latencies = append(latencies, l)
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
			t0 = time.Now()
		}
	}
	for ; putIdx < len(puts); putIdx++ {
		_, l := resolvePutCtx(puts[putIdx], putT0[putIdx], true)
		latencies = append(latencies, l)
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
