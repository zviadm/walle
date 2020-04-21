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
	"github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/walle/wallelib/topolib"
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
	totalMaxEntries := f.Int(
		"max_entries", 10*1000*1000,
		"Maximum number of entries, across all benchmarking streams. Will trim periodically to keep at the limit.")
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
	wg.Add(len(ws) + 1)
	go func() {
		benchStreamTrimmer(ctx, rootPb, clusterURI, *uriPrefix, *nStreams, *totalMaxEntries)
		wg.Done()
	}()
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

func putBatch(
	ctx context.Context,
	wIdx int,
	w *wallelib.Writer,
	qps int,
	sDist sizeDist) {

	progressN := 5 * qps
	maxInFlight := 10 * progressN
	puts := make(chan *wallelib.PutCtx, maxInFlight)
	putT0 := make(chan time.Time, maxInFlight)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		resolvePuts(w.StreamURI(), wIdx, qps, puts, putT0)
		wg.Done()
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	putN := 0
PutLoop:
	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			targetN := (i + 1) * qps / 100
			for ; putN < targetN; putN += 1 {
				size := sDist.RandSize()
				dataIdx := putN % (wallelib.MaxEntrySize - size + 1)
				putT0 <- time.Now()
				putCtx := w.PutEntry(benchData[dataIdx : dataIdx+size])
				puts <- putCtx
			}
		case <-ctx.Done():
			break PutLoop
		}
	}
	close(puts)
	wg.Wait()
	return
}

func resolvePuts(
	streamURI string, wIdx int, qps int,
	puts <-chan *wallelib.PutCtx,
	putT0 <-chan time.Time) {
	metricsKV := metrics.KV{"stream_uri": streamURI}
	totalPutsC := totalPutsCounter.V(metricsKV)
	putLatency99G := putLatency99Gauge.V(metricsKV)
	putLatency999G := putLatency999Gauge.V(metricsKV)

	progressN := 5 * qps
	latencies := make([]time.Duration, 0, progressN)
	putIdx := 0
	putTotalSize := 0
	t0 := time.Now()
	printProgress := func(pCtx *wallelib.PutCtx) {
		tDelta := time.Now().Sub(t0)
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p99 := latencies[len(latencies)*99/100]
		p999 := latencies[len(latencies)*999/1000]
		var entryId int64
		if pCtx != nil {
			entryId = pCtx.Entry.EntryId
		}
		zlog.Infof(
			"Bench[%d]: processed: %d, (entryId: %d) p50: %s p95: %s p99: %s p999: %s, QPS: %.2f (Target: %d), KB/s: %.1f",
			wIdx, putIdx, entryId,
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
	for putCtx := range puts {
		pt0 := <-putT0
		<-putCtx.Done()
		l := time.Now().Sub(pt0)
		exitOnErr(putCtx.Err())
		latencies = append(latencies, l)
		putTotalSize += len(putCtx.Entry.Data)
		putIdx += 1
		if putIdx >= progressN {
			printProgress(putCtx)
			latencies = latencies[:0]
			putIdx = 0
			putTotalSize = 0
			t0 = time.Now()
		}
	}
	printProgress(nil)
}

func benchStreamTrimmer(
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string,
	uriPrefix string,
	nStreams int,
	totalMaxEntries int) {

	root, err := wallelib.NewClientFromRootPb(ctx, rootPb, rootPb.RootUri)
	exitOnErr(err)
	topoMgr := topolib.NewClient(root)
	cli, err := wallelib.NewClientFromRootPb(ctx, rootPb, clusterURI)
	exitOnErr(err)
	maxEntriesPerStream := totalMaxEntries / nStreams
	for ctx.Err() == nil {
		for idx := 0; idx < nStreams; idx++ {
			streamURI := path.Join(uriPrefix, strconv.Itoa(idx))
			c, err := cli.ForStream(streamURI)
			exitOnErr(err)
			resp, err := c.PollStream(ctx, &walleapi.PollStreamRequest{StreamUri: streamURI})
			if err != nil {
				zlog.Errorf("[trimmer] err - %s", err)
				continue
			}
			trimTo := resp.EntryId - int64(maxEntriesPerStream)
			if trimTo <= 0 {
				continue
			}
			_, err = topoMgr.TrimStream(ctx, &topomgr.TrimStreamRequest{
				ClusterUri: clusterURI,
				StreamUri:  streamURI,
				EntryId:    trimTo,
			})
			if err != nil {
				zlog.Errorf("[trimmer] err - %s", err)
				continue
			}
			zlog.Infof("[trimmer] trimmed: %s, to: %d", streamURI, trimTo)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Minute):
			// Trimming every 5 minutes is very aggressive, but do it anyways to make benchmark
			// more aggressive than real world scenarios.
		}
	}
}
