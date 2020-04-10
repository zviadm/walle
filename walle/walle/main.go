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

	"github.com/zviadm/stats-go/exporters/datadog"
	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
)

var memBallast []byte

func main() {
	var storageDir = flag.String("walle.storage_dir", "", "Path where database and discovery data will be stored.")
	var host = flag.String(
		"walle.host", "",
		"Hostname that other servers can use to connect to this server. "+
			"If it isn't set, os.Hostname() will be used to determine it automatically.")
	var port = flag.String("walle.port", "", "Port to listen on.")

	var bootstrapRootURI = flag.String(
		"walle.bootstrap_uri", "",
		"Bootstrap new deployment. Will exit once new bootstrapped storage is created.")

	var clusterURI = flag.String("walle.cluster_uri", "", "Cluster URI to join.")

	// Tuning flags.
	var targetMemMB = flag.Int(
		"walle.target_mem_mb", 200, "Target maximum total memory usage.")
	var maxLocalStreams = flag.Int(
		"walle.max_local_streams", 10, "Maximum number of streams that this server can handle.")
	flag.Parse()
	ctx, cancelAll := context.WithCancel(context.Background())
	var cancelDeadline atomic.Value
	cancelDeadline.Store(time.Time{})

	err := datadog.ExporterGo(ctx)
	fatalOnErr(err)

	if *storageDir == "" {
		zlog.Fatal("must provide path to the storage using -walle.db_path flag")
	}
	dbPath := path.Join(*storageDir, "walle.db")
	rootFile := path.Join(*storageDir, "root.pb")
	topoFile := path.Join(*storageDir, "topology.pb")
	if *host == "" {
		hostname, err := os.Hostname()
		fatalOnErr(err)
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
		streamQueueMB = wallelib.MaxInFlightSize / 1024 / 1024
		// TODO(zviad): Produce a warning that target memory might not be enough for
		// all queues.
	}
	// Create memory ballast. TODO(zviad): Adjust this in future to be more dynamic if more
	// space is needed for queues.
	memBallast = make([]byte, *targetMemMB*1024*1024/8)

	zlog.Infof("initializing storage: %s...", dbPath)
	ss, err := storage.Init(dbPath, storage.InitOpts{
		Create:          true,
		CacheSizeMB:     cacheSizeMB,
		MaxLocalStreams: *maxLocalStreams,
	})
	fatalOnErr(err)
	zlog.Infof("initialized storage: %s", ss.ServerId())
	defer func() {
		time.Sleep(cancelDeadline.Load().(time.Time).Sub(time.Now()))
		zlog.Infof("closing storage...")
		ss.Close()
		zlog.Infof("storage closed and flushed")
	}()

	if *bootstrapRootURI != "" {
		err := walle.BootstrapRoot(ss, *bootstrapRootURI, rootFile, serverInfo)
		fatalOnErr(err)
		zlog.Infof(
			"bootstrapped %s, server: %s - %s",
			*bootstrapRootURI, ss.ServerId(), serverInfo)
		return
	}

	rootPb, err := wallelib.TopologyFromFile(rootFile)
	fatalOnErr(err)
	servingRootURI := *clusterURI == rootPb.RootUri
	rootD, err := wallelib.NewRootDiscovery(ctx, rootPb, !servingRootURI)
	fatalOnErr(err)
	rootCli := wallelib.NewClient(ctx, rootD)
	go watchTopologyAndSave(ctx, rootD, rootFile)
	var d wallelib.Discovery
	var c walle.Client
	if servingRootURI {
		d = rootD
		c = rootCli
	} else {
		topology, _ := wallelib.TopologyFromFile(topoFile) // ok to ignore errors.
		d, err = wallelib.NewDiscovery(ctx, rootCli, *clusterURI, topology)
		fatalOnErr(err)
		go watchTopologyAndSave(ctx, d, topoFile)
		c = wallelib.NewClient(ctx, d)
	}
	topology, _ := d.Topology()
	err = registerServerInfo(ctx, rootCli, *clusterURI, topology, ss.ServerId(), serverInfo)
	fatalOnErr(err)

	var topoMgr *topomgr.Manager
	if servingRootURI {
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
	fatalOnErr(err)
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
	err := s.Serve(l)
	fatalOnErr(err)
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

func fatalOnErr(err error) {
	if err != nil {
		zlog.Fatal(err)
	}
}

func registerServerInfo(
	ctx context.Context,
	root wallelib.Client,
	clusterURI string,
	topology *walleapi.Topology,
	serverId string,
	serverInfo *walleapi.ServerInfo) error {
	existingServerInfo, ok := topology.GetServers()[serverId]
	if ok && proto.Equal(existingServerInfo, serverInfo) {
		return nil
	}
	// Must register new server before it can start serving anything.
	// If registration fails, there is no point in starting up.
	zlog.Infof("updating serverInfo: %s -> %s (%s)...", existingServerInfo, serverInfo, clusterURI)
	topoMgr := topomgr.NewClient(root)
	_, err := topoMgr.RegisterServer(ctx, &topomgr_pb.RegisterServerRequest{
		ClusterUri: clusterURI,
		ServerId:   serverId,
		ServerInfo: serverInfo,
	})
	return err
}
