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
	if len(flag.Args()) == 0 {
		flag.Usage()
		os.Exit(1)
	}
	cmd := flag.Args()[0]

	rootPb, err := wallelib.RootPbFromEnv()
	exitOnErr(err)
	ctx := context.Background()
	var clusterURI string
	if *clusterName == "" {
		clusterURI = rootPb.RootUri
	} else {
		clusterURI = topomgr.Prefix + *clusterName
	}
	c, err := wallelib.NewClientFromRootPb(ctx, rootPb, clusterURI)
	exitOnErr(err)
	topoMgr := topomgr.NewClient(c)
	switch cmd {
	case "streams":
		t, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{ClusterUri: clusterURI})
		exitOnErr(err)
		for streamURI, streamT := range t.Streams {
			fmt.Printf("%s - %s\n", streamURI, streamT.ServerIds)
		}
	case "servers":
		t, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{ClusterUri: clusterURI})
		exitOnErr(err)
		for serverId, sInfo := range t.Servers {
			fmt.Printf("%s - %s\n", serverId, sInfo.Address)
		}
	//case "create":

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
