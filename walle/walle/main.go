package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"path"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc"

	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
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
	var clusterURI = flag.String("walle.cluster_uri", "",
		"Cluster URI for this server. If not set, value from -walle.root_uri will be used.")

	// Tuning flags.
	var targetMemMB = flag.Int(
		"walle.target_mem_mb", 200, "Target total memory usage.")
	var maxLocalStreams = flag.Int(
		"walle.max_local_streams", 10, "Maximum number of streams that this server can handle.")
	flag.Parse()
	ctx, cancelAll := context.WithCancel(context.Background())
	var cancelDeadline atomic.Value
	cancelDeadline.Store(time.Time{})

	if *rootURI == "" {
		zlog.Fatal("must provide root streamURI using -walle.root_uri flag")
	}
	if *storageDir == "" {
		zlog.Fatal("must provide path to the storage using -walle.db_path flag")
	}
	dbPath := path.Join(*storageDir, "walle.db")
	rootFile := path.Join(*storageDir, "root.pb")
	topoFile := path.Join(*storageDir, "topology.pb")
	if *host == "" {
		hostname, err := os.Hostname()
		if err != nil {
			zlog.Fatal(err)
		}
		*host = hostname
	}
	if *port == "" {
		zlog.Fatal("must provide port to listen on using -walle.port flag")
	}
	serverInfo := &walleapi.ServerInfo{Address: net.JoinHostPort(*host, *port)}

	// Memory allocation:
	// 50% goes to WT Cache. (non-GO memory)
	// 25% goes to per stream queue.
	// 25% goes to GC overhead.
	cacheSizeMB := *targetMemMB / 2
	streamQueueMB := *targetMemMB / 4 / (*maxLocalStreams)
	debug.SetGCPercent(100)
	if streamQueueMB*1024*1024 <= wallelib.MaxInFlightSize {
		zlog.Fatal(
			"not enough memory available for per stream queue (need 4MB). " +
				"either increase -walle.target_mem_mb, or decrease -walle.max_local_streams")
	}

	zlog.Infof("initializing storage: %s...", dbPath)
	ss, err := storage.Init(dbPath, storage.InitOpts{
		Create:          true,
		CacheSizeMB:     cacheSizeMB,
		MaxLocalStreams: *maxLocalStreams,
	})
	if err != nil {
		zlog.Fatal(err)
	}
	defer func() {
		time.Sleep(cancelDeadline.Load().(time.Time).Sub(time.Now()))
		zlog.Infof("closing storage...")
		ss.Close()
		zlog.Infof("storage closed and flushed")
	}()

	if *bootstrapOnly {
		err := walle.BootstrapRoot(ss, *rootURI, rootFile, serverInfo)
		if err != nil {
			zlog.Fatal(err)
		}
		zlog.Infof(
			"bootstrapped %s, server: %s - %s",
			*rootURI, ss.ServerId(), serverInfo)
		return
	}

	zlog.Infof("initializing root discovery: %s - %s...", *rootURI, rootFile)
	rootTopology, err := wallelib.TopologyFromFile(rootFile)
	if err != nil {
		zlog.Fatal(err)
	}
	rootD, err := wallelib.NewRootDiscovery(ctx, rootTopology)
	if err != nil {
		zlog.Fatal(err)
	}
	rootCli := wallelib.NewClient(ctx, rootD)
	go watchTopologyAndSave(ctx, rootD, rootFile)
	var d wallelib.Discovery
	var c walle.Client
	if *clusterURI == "" || *clusterURI == *rootURI {
		*clusterURI = *rootURI
		d = rootD
		c = rootCli
	} else {
		zlog.Infof("initializing discovery: %s...", *clusterURI)
		topology, _ := wallelib.TopologyFromFile(topoFile) // ok to ignore errors.
		d, err = wallelib.NewDiscovery(ctx, rootCli, *clusterURI, topology)
		if err != nil {
			zlog.Fatal(err)
		}
		go watchTopologyAndSave(ctx, d, topoFile)
		c = wallelib.NewClient(ctx, d)
	}
	err = registerServerInfo(
		ctx,
		rootCli, *clusterURI, d,
		ss.ServerId(), serverInfo)
	if err != nil {
		zlog.Fatal(err)
	}

	var topoMgr *topomgr.Manager
	if *rootURI == *clusterURI {
		topoMgr = topomgr.NewManager(rootCli, serverInfo.Address)
	}
	ws := walle.NewServer(ctx, ss, c, d, streamQueueMB*1024*1024, topoMgr)
	s := grpc.NewServer()
	walle_pb.RegisterWalleServer(s, ws)
	walleapi.RegisterWalleApiServer(s, ws)
	if topoMgr != nil {
		topomgr_pb.RegisterTopoManagerServer(s, topoMgr)
	}

	l, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		zlog.Fatal(err)
	}
	notify := make(chan os.Signal, 10)
	signal.Notify(notify, syscall.SIGTERM)
	go func() {
		<-notify
		zlog.Infof("starting graceful shutdown...")
		cancelAll()
		cancelDeadline.Store(time.Now().Add(time.Second))
		s.GracefulStop()
	}()
	zlog.Infof("starting server on port:%s...", *port)
	if err := s.Serve(l); err != nil {
		zlog.Fatal(err)
	}
}

func watchTopologyAndSave(ctx context.Context, d wallelib.Discovery, f string) {
	for {
		t, notify := d.Topology()
		if err := wallelib.TopologyToFile(t, f); err != nil {
			zlog.Warningf("err saving topology: %s", err)
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
	root wallelib.Client,
	clusterURI string,
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
	zlog.Infof("updating serverInfo: %s -> %s ...", existingServerInfo, serverInfo)
	topoMgr := topomgr.NewClient(root)
	_, err := topoMgr.UpdateServerInfo(ctx, &topomgr_pb.UpdateServerInfoRequest{
		ClusterUri: clusterURI,
		ServerId:   serverId,
		ServerInfo: serverInfo,
	})
	return err
}
