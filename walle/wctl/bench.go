package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"strconv"
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
	uriPrefix := f.String("prefix", "/bench", "TODO(zviad):")
	nStreams := f.Int("streams", 1, "TODO(zviad):")
	nInflight := f.Int("inflight", 1, "TODO(zviad):")
	f.Parse(args)

	c, err := wallelib.NewClientFromRootPb(ctx, rootPb, clusterURI)
	exitOnErr(err)

	w := make([]*wallelib.Writer, *nStreams)
	for idx := range w {
		w[idx], err = wallelib.WaitAndClaim(
			ctx, c, *uriPrefix+"/"+strconv.Itoa(idx), "bench:0000", time.Second)
		exitOnErr(err)
	}
	fmt.Println("writers claimed, starting benchmark...")
	putBatch(*nInflight, w...)
}

func putBatch(
	maxInFlight int,
	ws ...*wallelib.Writer) time.Duration {

	nBatch := 1000 * len(ws)
	maxInFlight *= len(ws)
	puts := make([]*wallelib.PutCtx, 0, nBatch+maxInFlight)
	putT0 := make([]time.Time, 0, nBatch+maxInFlight)
	latencies := make([]time.Duration, 0, nBatch)
	putIdx := 0

	t0 := time.Now()
	for i := 0; ; i++ {
		putCtx := ws[i%len(ws)].PutEntry([]byte("testingoooo " + strconv.Itoa(i)))
		puts = append(puts, putCtx)
		putT0 = append(putT0, time.Now())

		ok, l := resolvePutCtx(puts[putIdx], putT0[putIdx], i-putIdx+1 >= maxInFlight)
		if ok {
			latencies = append(latencies, l)
			putIdx += 1
		}

		if putIdx == nBatch {
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			zlog.Info(
				"Bench: processed: ", putIdx, " (entryId: ", puts[putIdx-1].Entry.EntryId, ")",
				" writers: ", len(ws), " inflight: ", maxInFlight, " ",
				" p50: ", latencies[len(latencies)*50/100],
				" p95: ", latencies[len(latencies)*95/100],
				" p99: ", latencies[len(latencies)*99/100],
				" p999: ", latencies[len(latencies)*999/1000],
				" total: ", time.Now().Sub(t0),
			)
			copy(puts, puts[putIdx:])
			puts = puts[:len(puts)-putIdx]
			copy(putT0, putT0[putIdx:])
			putT0 = putT0[:len(putT0)-putIdx]
			latencies = latencies[:0]
			putIdx = 0
			t0 = time.Now()
		}
	}
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
