package walle

import (
	"context"
	"testing"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
)

func TestPipelineQueue(t *testing.T) {
	_ = newPipelineQueue()
	// q.Push(&walle_pb.PutEntryInternalRequest{})

}

var topo1Node = &walleapi.Topology{
	Streams: map[string]*walleapi.StreamTopology{
		"/mock/1": &walleapi.StreamTopology{
			Version:   3,
			ServerIds: []string{"\x00\x01"},
		},
	},
	Servers: map[string]*walleapi.ServerInfo{
		"\x00\x01": &walleapi.ServerInfo{Address: "localhost1:1001"},
	},
}

func BenchmarkPutEntrySerial(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, topo1Node, storage.TestTmpDir())

	w, _, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ee := w.PutEntry([]byte("testingoooo"))
		<-ee.Done()
		if ee.Err() != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutEntryPipeline(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, topo1Node, storage.TestTmpDir())

	w, _, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	b.ResetTimer()
	puts := make([]*wallelib.PutCtx, b.N)
	for i := 0; i < b.N; i++ {
		puts[i] = w.PutEntry([]byte("testingoooo"))
	}
	for _, putCtx := range puts {
		<-putCtx.Done()
		if putCtx.Err() != nil {
			b.Fatal(err)
		}
	}
}
