package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math"
	mrand "math/rand"
	"os"
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
	dist := f.String("dist", "log_normal", "Entry size distribution. Options are: log_normal, uniform.")
	totalTime := f.Duration("time", 0, "Total bench duration. If 0, will run forever.")
	f.Parse(args)

	metrics.SetInstanceName("wctl")
	err := datadog.ExporterGo(ctx)
	exitOnErr(err)

	memBallast = make([]byte, *throughputKBs*1024*5)

	c, err := wallelib.NewClientFromRootPb(ctx, rootPb, clusterURI)
	exitOnErr(err)

	_, err = rand.Read(benchData)
	exitOnErr(err)
	var sDist sizeDist
	sizeAvg := (*throughputKBs) * 1024 / (*qps)
	switch *dist {
	case "log_normal":
		// Log normal distribution.
		// Median: sizeAvg / 2
		// Mean: sizeAvg
		mu := math.Log(float64(sizeAvg) / 2)
		sigma := math.Sqrt(2 * (math.Log(float64(sizeAvg)) - mu))
		sDist = &normalDist{mu: mu, sigma: sigma}
	case "uniform":
		// Uniform distribution: [sizeAvg/2 ... sizeAvg + sizeAvg/2]
		sDist = &uniformDist{min: sizeAvg / 2, max: sizeAvg * 3 / 2}
	default:
		log.Println("unknown type:", *dist)
		os.Exit(1)
	}

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
	wg := sync.WaitGroup{}
	wg.Add(len(ws))
	for idx, w := range ws {
		go func(idx int, w *wallelib.Writer) {
			putBatch(ctx, idx, w, *qps/len(ws), sDist)
			wg.Done()
		}(idx, w)
	}
	wg.Wait()
}

func boundSize(size int) int {
	if size < 0 {
		return 0
	}
	if size > wallelib.MaxEntrySize {
		return wallelib.MaxEntrySize
	}
	return size
}

type sizeDist interface {
	RandSize() int
}

type normalDist struct {
	mu    float64
	sigma float64
}

func (d *normalDist) RandSize() int {
	rnd := mrand.NormFloat64()
	size := int(math.Exp(rnd*d.sigma + d.mu))
	return boundSize(size)
}

type uniformDist struct {
	min int
	max int
}

func (d *uniformDist) RandSize() int {
	return boundSize(d.min + mrand.Intn(d.max-d.min))
}

func newBufferredTicker(ctx context.Context, d time.Duration, buffer int) <-chan struct{} {
	c := make(chan struct{}, buffer)
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				close(c)
				return
			case <-ticker.C:
				c <- struct{}{}
			}
		}
	}()
	return c
}

func putBatch(
	ctx context.Context,
	wIdx int,
	w *wallelib.Writer,
	qps int,
	sDist sizeDist) {

	metricsKV := metrics.KV{"stream_uri": w.StreamURI()}
	totalPutsC := totalPutsCounter.V(metricsKV)
	putLatency99G := putLatency99Gauge.V(metricsKV)
	putLatency999G := putLatency999Gauge.V(metricsKV)

	progressN := 5 * qps
	maxInFlight := 10 * progressN

	puts := make([]*wallelib.PutCtx, 0, maxInFlight)
	putT0 := make([]time.Time, 0, maxInFlight)
	latencies := make([]time.Duration, 0, maxInFlight)
	putIdx := 0
	putTotalSize := 0

	t0 := time.Now()
	printProgress := func() {
		tDelta := time.Now().Sub(t0)
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		sort.Slice(puts[:putIdx], func(i, j int) bool { return puts[i].Entry.Size() < puts[j].Entry.Size() })
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

	tickerC := newBufferredTicker(ctx, time.Second/time.Duration(qps), maxInFlight)
	for i := 0; ctx.Err() == nil; i++ {
		var putDone <-chan struct{}
		if putIdx < len(puts) {
			putDone = puts[putIdx].Done()
		}
		resolveCtx := false
		select {
		case <-tickerC:
			tickerN := 1
		DrainTicker:
			for {
				select {
				case <-tickerC:
					tickerN += 1
				default:
					break DrainTicker
				}
			}
			for i := 0; i < tickerN; i++ {
				size := sDist.RandSize()
				dataIdx := i % (wallelib.MaxEntrySize - size + 1)
				putCtx := w.PutEntry(benchData[dataIdx : dataIdx+size])
				puts = append(puts, putCtx)
				putT0 = append(putT0, time.Now())
			}
		case <-putDone:
			resolveCtx = true
		}
		resolveCtx = resolveCtx || len(puts) >= maxInFlight
		if resolveCtx {
			_, l := resolvePutCtx(puts[putIdx], putT0[putIdx], true)
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
