package main

import (
	"context"
	"flag"
	"net"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"google.golang.org/grpc"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/wallelib"
)

func main() {
	var port = flag.Int("walle.port", 5005, "")
	var rootURI = flag.String("walle.root_uri", "", "Root topology URI for the cluster.")
	// TODO(zviad): This should be read from a file too, or some place dynamic.
	var rootSeeds = flag.String("walle.root_seeds", "",
		"Comma separated list of <seed id>:<seed addr> pairs for servers serving the root topology URI")
	var topologyURI = flag.String("walle.topology_uri", "", "Topology URI for this server.")
	var dbPath = flag.String("walle.db_path", "", "Path where database will be stored.")

	// var rootBootstrapEntry = flag.String("walle.bootstrap_entry", "", "")
	flag.Parse()

	ctx := context.Background()
	seedAddrPairs := strings.Split(*rootSeeds, ",")
	seedAddrs := make(map[string]string, len(seedAddrPairs))
	for _, seedAddrPair := range seedAddrPairs {
		pair := strings.SplitN(seedAddrPair, ":", 2)
		seedAddrs[pair[0]] = pair[1]
	}
	rootD := wallelib.NewRootDiscovery(ctx, *rootURI, seedAddrs)
	var d wallelib.Discovery
	if *topologyURI == *rootURI {
		d = rootD
	} else {
		rootCli := wallelib.NewClient(ctx, rootD)
		d = wallelib.NewDiscovery(ctx, rootCli, *rootURI, *topologyURI)
	}
	c := wallelib.NewClient(ctx, d)

	ss, err := walle.StorageInit(*dbPath, true)
	if err != nil {
		glog.Fatalf("failed to initialize storage: %v", err)
	}
	defer ss.Close()
	ws := walle.NewServer(ctx, ss, c, d)
	s := grpc.NewServer()
	walle_pb.RegisterWalleServer(s, ws)

	l, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		glog.Fatalf("failed to listen: %v", err)
	}
	glog.Infof("starting WALLE server on port:%d...", *port)
	if err := s.Serve(l); err != nil {
		glog.Fatalf("failed to serve: %v", err)
	}
}
