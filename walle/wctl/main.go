package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/walle/wallelib/topolib"
)

type cmdFunc func(ctx context.Context, rootPb *walleapi.Topology, clusterURI string, args []string)

var _CMDs = map[string]cmdFunc{
	"streams":  cmdStreams,
	"servers":  cmdServers,
	"crupdate": cmdCrupdate,
	"bench":    cmdBench,
	"scan":     cmdScan,
	"trim":     cmdTrim,
}

func main() {
	clusterName := flag.String(
		"c", "",
		"Cluster to operate on. Can be just the name or thhe full /cluster/<name> URI. "+
			"If left empty, will operate on root cluster by default.")
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootPb, err := wallelib.RootPbFromEnv()
	exitOnErr(err)
	var clusterURI string
	if *clusterName == "" {
		clusterURI = rootPb.RootUri
	} else {
		clusterURI = *clusterName
		if !strings.HasPrefix(clusterURI, topomgr.Prefix) {
			clusterURI = path.Join(topomgr.Prefix, clusterURI)
		}
	}

	args := flag.Args()
	if len(args) == 0 {
		var cmds sort.StringSlice
		for cmd := range _CMDs {
			cmds = append(cmds, cmd)
		}
		cmds.Sort()
		fmt.Println("must provide command to run: ", strings.Join(cmds, ","))
		os.Exit(1)
	}
	cmd, args := args[0], args[1:]
	cmdF, ok := _CMDs[cmd]
	if !ok {
		fmt.Println("unknown command: ", cmd)
		os.Exit(1)
	}
	cmdF(ctx, rootPb, clusterURI, args)
}

func cmdStreams(
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string,
	args []string) {
	root, err := wallelib.NewClientFromRootPb(ctx, rootPb, rootPb.RootUri)
	exitOnErr(err)
	topoMgr := topolib.NewClient(root)
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
}
func cmdServers(
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string,
	args []string) {
	root, err := wallelib.NewClientFromRootPb(ctx, rootPb, rootPb.RootUri)
	exitOnErr(err)
	topoMgr := topolib.NewClient(root)
	t, err := topoMgr.FetchTopology(ctx, &topomgr_pb.FetchTopologyRequest{ClusterUri: clusterURI})
	exitOnErr(err)
	var serverIds sort.StringSlice
	for serverId := range t.Servers {
		serverIds = append(serverIds, serverId)
	}
	serverIds.Sort()
	for _, serverId := range serverIds {
		fmt.Printf("%s - %s\n", serverId, t.Servers[serverId].Address)
	}
}
func cmdCrupdate(
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string,
	args []string) {
	if len(args) < 1 {
		fmt.Println("must provide stream URI to create or update")
		os.Exit(1)
	}
	streamURI, args := args[0], args[1:]
	serverIds := args

	root, err := wallelib.NewClientFromRootPb(ctx, rootPb, rootPb.RootUri)
	exitOnErr(err)
	topoMgr := topolib.NewClient(root)
	_, err = topoMgr.CrUpdateStream(
		ctx, &topomgr_pb.CrUpdateStreamRequest{
			ClusterUri: clusterURI,
			StreamUri:  streamURI,
			ServerIds:  serverIds,
		})
	exitOnErr(err)
	fmt.Printf("stream: %s, members: %s\n", streamURI, serverIds)
}
func cmdTrim(
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string,
	args []string) {
	if len(args) != 2 {
		fmt.Println("must provide stream_uri and entry_id to trim!")
		os.Exit(1)
	}
	streamURI := args[0]
	entryId, err := strconv.Atoi(args[1])
	exitOnErr(err)

	root, err := wallelib.NewClientFromRootPb(ctx, rootPb, rootPb.RootUri)
	exitOnErr(err)
	topoMgr := topolib.NewClient(root)
	_, err = topoMgr.TrimStream(
		ctx, &topomgr_pb.TrimStreamRequest{
			ClusterUri: clusterURI,
			StreamUri:  streamURI,
			EntryId:    int64(entryId),
		})
	exitOnErr(err)
	fmt.Printf("stream: %s, trimmed to: %d\n", streamURI, entryId)
}

func exitOnErr(err error) {
	if err == nil {
		return
	}
	fmt.Println(err.Error())
	os.Exit(1)
}
