package pipeline

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
)

func fakeFetch(
	ctx context.Context,
	streamURI string,
	committedId int64,
	committedXX uint64) (*walleapi.Entry, error) {
	return nil, errors.New("not implemented")
}

// BenchmarkFullPipeline_1-4 - 1.00 cgocalls/op	- 4 allocs/op
func BenchmarkFullPipeline_1(b *testing.B) {
	benchmarkFullPipeline(b, 1)
}

// BenchmarkFullPipeline_10-4 - 1.00 cgocalls/op - 4 allocs/op
func BenchmarkFullPipeline_10(b *testing.B) {
	benchmarkFullPipeline(b, 100)
}

// BenchmarkFullPipeline_100-4 - 1.00 cgocalls/op - 4 allocs/op
func BenchmarkFullPipeline_100(b *testing.B) {
	benchmarkFullPipeline(b, 100)
}

func benchmarkFullPipeline(b *testing.B, nStreams int) {
	tmpDir := storage.TestTmpDir()
	s, err := storage.Init(tmpDir, storage.InitOpts{Create: true, MaxLocalStreams: nStreams})
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := New(ctx, s.FlushSync, fakeFetch)
	var streams []storage.Stream
	for idx := 0; idx < nStreams; idx++ {
		uri := "/test/" + strconv.Itoa(idx)
		err := s.UpsertStream(
			uri, &walleapi.StreamTopology{Version: 1, ServerIds: []string{s.ServerId()}})
		require.NoError(b, err)
		ss, _ := s.Stream(uri)
		streams = append(streams, ss)
	}
	var entries []*walleapi.Entry
	entry := storage.Entry0
	for i := 0; i < b.N/nStreams+1; i++ {
		checksum := wallelib.CalculateChecksumXX(entry.ChecksumXX, entry.Data)
		entry = &walleapi.Entry{
			EntryId:    entry.EntryId + 1,
			WriterId:   entry.WriterId,
			ChecksumXX: checksum,
			Data:       entry.Data,
		}
		entries = append(entries, entry)
	}
	var rs []*ResultCtx

	cgoCalls0 := runtime.NumCgoCall()
	b.ResetTimer()
	b.ReportAllocs()
	reverseN := 10
	for i := 0; i < b.N; i++ {
		sIdx := i % nStreams
		eIdx := (i / nStreams)
		eIdxStart := eIdx / reverseN * reverseN
		eIdxOffset := eIdx - eIdxStart
		if eIdxStart+reverseN <= len(entries) {
			eIdxOffset = reverseN - 1 - eIdxOffset
		}
		eIdx = eIdxStart + eIdxOffset

		r := p.ForStream(streams[sIdx]).QueuePut(entries[eIdx], eIdx%1000 == 0)
		rs = append(rs, r)
	}
	for idx, r := range rs {
		<-r.Done()
		if r.Err() != nil {
			b.Fatalf("err: %d %d -- %s", idx, b.N, r.Err())
		}
	}
	b.ReportMetric(float64(runtime.NumCgoCall()-cgoCalls0)/float64(b.N), "cgocalls/op")
}
