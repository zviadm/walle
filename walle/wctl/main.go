package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/pkg/errors"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
)

func main() {
	clusterName := flag.String("c", "", "Cluster to operate on.")
	flag.Parse()
	if len(flag.Args()) != 0 {
		flag.Usage()
		os.Exit(1)
	}
	cmd := flag.Args()[0]

	rootPb, err := wallelib.RootPbFromEnv()
	exitOnErr(err)
	ctx := context.Background()
	topologyURI := topomgr.Prefix + *clusterName
	c, err := wallelib.NewClientFromRootPb(ctx, rootPb, topologyURI)
	exitOnErr(err)
	topoMgr := topomgr.NewClient(c)
	switch cmd {
	case "show":
		t, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{TopologyUri: topologyURI})
		exitOnErr(err)
		fmt.Println(t)
	default:
		exitOnErr(errors.Errorf("unknown command: %s", cmd))
	}
}

func exitOnErr(err error) {
	if err == nil {
		return
	}
	fmt.Println(err.Error())
	os.Exit(1)
}
