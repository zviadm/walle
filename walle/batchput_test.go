package walle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
)

var topo1Node = &walleapi.Topology{
	Streams: map[string]*walleapi.StreamTopology{
		"/mock/1": &walleapi.StreamTopology{
			Version:   3,
			ServerIds: []string{"01"},
		},
	},
	Servers: map[string]*walleapi.ServerInfo{
		"01": &walleapi.ServerInfo{Address: "localhost1:1001"},
	},
}

var (
	benchData = []byte("test data for benchmarking")
)

// BenchmarkPutEntrySerial-4 - 772 - 1396476 ns/op - 3194 B/op - 59 allocs/op
func BenchmarkPutEntrySerial(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, topo1Node, storage.TestTmpDir())

	w, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", time.Second)
	require.NoError(b, err)
	defer w.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		putCtx := w.PutEntry(benchData)
		<-putCtx.Done()
		if err := putCtx.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutEntryPipeline-4 - 6530 - 181576 ns/op - 2669 B/op - 48 allocs/op
func BenchmarkPutEntryPipeline(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, topo1Node, storage.TestTmpDir())

	w, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	b.ResetTimer()
	b.ReportAllocs()
	puts := make([]*wallelib.PutCtx, b.N)
	for i := 0; i < b.N; i++ {
		puts[i] = w.PutEntry(benchData)
	}
	for _, putCtx := range puts {
		<-putCtx.Done()
		if err := putCtx.Err(); err != nil {
			b.Fatal(err)
		}
	}
}
