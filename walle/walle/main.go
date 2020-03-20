package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

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
	if *rootURI == "" {
		glog.Fatal("must provide root streamURI using -walle.root_uri flag")
	}
	if *topologyURI == "" {
		glog.Fatal("must provide topology streamURI using -walle.topology_uri flag")
	}
	if *dbPath == "" {
		glog.Fatal("must provide path to the database using -walle.db_path flag")
	}
	glog.Infof("initializing storage: %s...", *dbPath)
	ss, err := walle.StorageInit(*dbPath, true)
	if err != nil {
		glog.Fatalf("failed to initialize storage: %v", err)
	}
	defer ss.Close()

	if *rootSeeds == "" {
		glog.Fatal("must provide seed addresses for root servers using -walle.root_seeds flag")
	}
	seedAddrPairs := strings.Split(*rootSeeds, ",")
	seedAddrs := make(map[string]string)
	for _, seedAddrPair := range seedAddrPairs {
		pair := strings.SplitN(seedAddrPair, ":", 2)
		seedAddrs[pair[0]] = pair[1]
	}
	glog.Infof("initializing root discovery: %s - %v...", *rootURI, seedAddrs)
	rootD := wallelib.NewRootDiscovery(ctx, *rootURI, seedAddrs)
	var d wallelib.Discovery
	if *topologyURI == *rootURI {
		d = rootD
	} else {
		rootCli := wallelib.NewClient(ctx, rootD)
		glog.Infof("initializing topology discovery: %s...", *topologyURI)
		d = wallelib.NewDiscovery(ctx, rootCli, *rootURI, *topologyURI)
	}
	c := wallelib.NewClient(ctx, d)

	ws := walle.NewServer(ctx, ss, c, d)
	s := grpc.NewServer()
	walle_pb.RegisterWalleServer(s, ws)

	l, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		glog.Fatalf("failed to listen: %v", err)
	}
	notify := make(chan os.Signal, 10)
	signal.Notify(notify, syscall.SIGTERM)
	go func() {
		<-notify
		glog.Infof("terminating WALLE server...")
		s.GracefulStop()
	}()
	glog.Infof("starting WALLE server on port:%d...", *port)
	if err := s.Serve(l); err != nil {
		glog.Fatalf("failed to serve: %v", err)
	}
}
