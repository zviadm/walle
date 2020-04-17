package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/pkg/errors"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/walle/wallelib/topolib"
)

func main() {
	clusterName := flag.String("c", "", "Cluster to operate on. Can be just the name or full /cluster/<name> URI.")
	flag.Parse()
	rootPb, err := wallelib.RootPbFromEnv()
	exitOnErr(err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var clusterURI string
	if *clusterName == "" {
		clusterURI = rootPb.RootUri
	} else {
		clusterURI = *clusterName
		if !strings.HasPrefix(clusterURI, topomgr.Prefix) {
			clusterURI = path.Join(topomgr.Prefix, clusterURI)
		}
	}
	root, err := wallelib.NewClientFromRootPb(ctx, rootPb, rootPb.RootUri)
	exitOnErr(err)
	topoMgr := topolib.NewClient(root)

	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("must provide command to run")
		os.Exit(1)
	}
	cmd, args := args[0], args[1:]
	switch cmd {
	case "streams":
		t, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{ClusterUri: clusterURI})
		exitOnErr(err)
		streamURIs := make(sort.StringSlice, 0, len(t.Streams))
		for streamURI := range t.Streams {
			streamURIs = append(streamURIs, streamURI)
		}
		streamURIs.Sort()
		for _, streamURI := range streamURIs {
			fmt.Printf("%s - %s - v:%d\n", streamURI, t.Streams[streamURI].ServerIds, t.Streams[streamURI].Version)
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
		_, err := topoMgr.CrUpdateStream(
			ctx, &topomgr_pb.CrUpdateStreamRequest{
				ClusterUri: clusterURI,
				StreamUri:  streamURI,
				ServerIds:  serverIds,
			})
		exitOnErr(err)
		fmt.Printf("stream: %s, members: %s\n", streamURI, serverIds)
	case "bench":
		cmdBench(ctx, rootPb, clusterURI, args)
	case "scan":
		cmdScan(ctx, rootPb, clusterURI, args)
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
