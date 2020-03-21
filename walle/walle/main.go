package main

import (
	"context"
	"encoding/hex"
	"flag"
	"net"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/golang/glog"
	"google.golang.org/grpc"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/wallelib"
)

func main() {
	var rootURI = flag.String("walle.root_uri", "", "Root topology URI for the cluster.")
	var storageDir = flag.String("walle.storage_dir", "", "Path where database and discovery data will be stored.")
	var host = flag.String(
		"walle.host", "",
		"Hostname that other servers can use to connect to this server. "+
			"If it isn't set, os.Hostname() will be used to determine it automatically.")
	var port = flag.String("walle.port", "", "Port to listen on.")
	var bootstrapOnly = flag.Bool(
		"walle.bootstrap_only", false,
		"Bootstrap new root topology. Will exit once new bootstrapped storage is created. "+
			"Should be restarted without -walle.bootstrap_only flag if it bootstraps successfully.")
	var topologyURI = flag.String("walle.topology_uri", "",
		"Topology URI for this server. If not set, value from -walle.root_uri will be used.")

	flag.Parse()
	ctx, cancelAll := context.WithCancel(context.Background())
	if *rootURI == "" {
		glog.Fatal("must provide root streamURI using -walle.root_uri flag")
	}
	if *storageDir == "" {
		glog.Fatal("must provide path to the storage using -walle.db_path flag")
	}
	dbPath := path.Join(*storageDir, "walle.db")
	rootTopoFile := path.Join(*storageDir, "root_topology.pb")
	topoFile := path.Join(*storageDir, "topology.pb")
	if *host == "" {
		hostname, err := os.Hostname()
		if err != nil {
			glog.Fatal(err)
		}
		*host = hostname
	}
	if *port == "" {
		glog.Fatal("must provide port to listen on using -walle.port flag")
	}
	addr := net.JoinHostPort(*host, *port)
	glog.Infof("initializing storage: %s...", dbPath)
	ss, err := walle.StorageInit(dbPath, true)
	if err != nil {
		glog.Fatalf("failed to initialize storage: %v", err)
	}
	defer ss.Close()

	if *bootstrapOnly {
		err := walle.BootstrapRoot(ss, *rootURI, rootTopoFile, addr)
		if err != nil {
			glog.Fatal(err)
		}
		glog.Infof(
			"bootstrapped %s, server: %s - %s",
			*rootURI, hex.EncodeToString([]byte(ss.ServerId())), addr)
		return
	}

	glog.Infof("initializing root discovery: %s - %v...", *rootURI, rootTopoFile)
	rootD, err := wallelib.NewRootDiscovery(ctx, *rootURI, rootTopoFile)
	if err != nil {
		glog.Fatal(err)
	}
	var d wallelib.Discovery
	if *topologyURI == "" || *topologyURI == *rootURI {
		d = rootD
	} else {
		rootCli := wallelib.NewClient(ctx, rootD)
		glog.Infof("initializing topology discovery: %s...", *topologyURI)
		d, err = wallelib.NewDiscovery(ctx, rootCli, *rootURI, *topologyURI, topoFile)
		if err != nil {
			glog.Fatal(err)
		}
	}
	c := wallelib.NewClient(ctx, d)

	ws := walle.NewServer(ctx, ss, c, d)
	s := grpc.NewServer()
	walle_pb.RegisterWalleServer(s, ws)
	walleapi.RegisterWalleApiServer(s, ws)

	l, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		glog.Fatalf("failed to listen: %v", err)
	}
	notify := make(chan os.Signal, 10)
	signal.Notify(notify, syscall.SIGTERM)
	go func() {
		<-notify
		glog.Infof("terminating WALLE server...")
		cancelAll()
		s.GracefulStop()
	}()
	glog.Infof("starting WALLE server on port:%s...", *port)
	if err := s.Serve(l); err != nil {
		glog.Fatalf("failed to serve: %v", err)
	}
}
