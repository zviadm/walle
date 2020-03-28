package walle

import (
	"context"
	"testing"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
)

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
	_, c := newMockSystem(ctx, topo1Node, TestTmpDir())

	w, _, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close(false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, errC := w.PutEntry([]byte("testingoooo"))
		err := <-errC
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutEntryPipeline(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, topo1Node, TestTmpDir())

	w, _, err := wallelib.WaitAndClaim(ctx, c, "/mock/1", "testhost:1001", wallelib.LeaseMinimum)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close(false)

	b.ResetTimer()
	errCs := make([]<-chan error, streamPipelineQ/2)
	for i := 0; i < b.N; i++ {
		_, errC := w.PutEntry([]byte("testingoooo"))
		if i >= len(errCs) {
			err := <-errCs[i%len(errCs)]
			if err != nil {
				b.Fatal(err)
			}
		}
		errCs[i%len(errCs)] = errC
	}
	for _, errC := range errCs {
		if errC == nil {
			continue
		}
		err := <-errC
		if err != nil {
			b.Fatal(err)
		}
	}
}
