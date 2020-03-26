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

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"google.golang.org/grpc"

	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/topomgr"
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
		"Bootstrap new deployment. Will exit once new bootstrapped storage is created. "+
			"Should be started again without -walle.bootstrap_only flag if it exits successfully.")
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
	rootFile := path.Join(*storageDir, "root.pb")
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
	serverInfo := &walleapi.ServerInfo{
		Address: net.JoinHostPort(*host, *port),
	}
	glog.Infof("initializing storage: %s...", dbPath)
	ss, err := walle.StorageInit(dbPath, true)
	if err != nil {
		glog.Fatal(err)
	}
	defer ss.Close()

	if *bootstrapOnly {
		err := walle.BootstrapRoot(ss, *rootURI, rootFile, serverInfo)
		if err != nil {
			glog.Fatal(err)
		}
		glog.Infof(
			"bootstrapped %s, server: %s - %s",
			*rootURI, hex.EncodeToString([]byte(ss.ServerId())), serverInfo)
		return
	}

	glog.Infof("initializing root discovery: %s - %s...", *rootURI, rootFile)
	rootTopology, err := wallelib.TopologyFromFile(rootFile)
	if err != nil {
		glog.Fatal(err)
	}
	rootD, err := wallelib.NewRootDiscovery(ctx, *rootURI, rootTopology)
	if err != nil {
		glog.Fatal(err)
	}
	rootCli := wallelib.NewClient(ctx, rootD)
	go watchTopologyAndSave(ctx, rootD, rootFile)
	var d wallelib.Discovery
	var c walle.Client
	if *topologyURI == "" || *topologyURI == *rootURI {
		*topologyURI = *rootURI
		d = rootD
		c = rootCli
	} else {
		glog.Infof("initializing topology discovery: %s...", *topologyURI)
		topology, _ := wallelib.TopologyFromFile(topoFile) // ok to ignore errors.
		d, err = wallelib.NewDiscovery(ctx, rootCli, *topologyURI, topology)
		if err != nil {
			glog.Fatal(err)
		}
		go watchTopologyAndSave(ctx, d, topoFile)
		c = wallelib.NewClient(ctx, d)
	}
	err = registerServerInfo(
		ctx,
		rootCli, *topologyURI, d,
		ss.ServerId(), serverInfo)
	if err != nil {
		glog.Fatal(err)
	}

	var topoMgr *topomgr.Manager
	if *rootURI == *topologyURI {
		topoMgr = topomgr.NewManager(rootCli, serverInfo.Address)
	}
	ws := walle.NewServer(ctx, ss, c, d, topoMgr)
	s := grpc.NewServer()
	walle_pb.RegisterWalleServer(s, ws)
	walleapi.RegisterWalleApiServer(s, ws)
	if topoMgr != nil {
		topomgr_pb.RegisterTopoManagerServer(s, topoMgr)
	}

	l, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		glog.Fatal(err)
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
		glog.Fatal(err)
	}
}

func watchTopologyAndSave(ctx context.Context, d wallelib.Discovery, f string) {
	for {
		t, notify := d.Topology()
		if err := wallelib.TopologyToFile(t, f); err != nil {
			glog.Warningf("saving topology to file failed: %s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-notify:
		}
	}
}

func registerServerInfo(
	ctx context.Context,
	root wallelib.BasicClient,
	topologyURI string,
	topologyD wallelib.Discovery,
	serverId string,
	serverInfo *walleapi.ServerInfo) error {
	topology, _ := topologyD.Topology()
	existingServerInfo, ok := topology.GetServers()[serverId]
	if ok && proto.Equal(existingServerInfo, serverInfo) {
		return nil
	}
	// Must register new server before it can start serving anything.
	// If registration fails, there is no point in starting up.
	glog.Infof("updating serverInfo: %s -> %s ...", existingServerInfo, serverInfo)
	topoMgr := topomgr.NewClient(root)
	_, err := topoMgr.RegisterServer(ctx, &topomgr_pb.RegisterServerRequest{
		TopologyUri: topologyURI,
		ServerId:    serverId,
		ServerInfo:  serverInfo,
	})
	return err
}
