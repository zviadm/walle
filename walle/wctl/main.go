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
	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("must provide command to run")
		os.Exit(1)
	}
	cmd, args := args[0], args[1:]

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
	case "create":
		if len(args) < 1 {
			fmt.Println("need to provide stream URI to create")
		}
		streamURI, args := args[0], args[1:]
		serverIds := args
		_, err := topoMgr.UpdateServerIds(
			ctx, &topomgr_pb.UpdateServerIdsRequest{
				ClusterUri: clusterURI,
				StreamUri:  streamURI,
				ServerIds:  serverIds,
			})
		exitOnErr(err)
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
