package main

import (
	"context"
	"flag"
	mrand "math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/zviadm/stats-go/exporters/datadog"
	"github.com/zviadm/stats-go/handlers/grpcstats"
	_ "github.com/zviadm/stats-go/handlers/runtimestats"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc"

	topomgr_pb "github.com/zviadm/walle/proto/topomgr"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/server"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/walle/topomgr"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/walle/wallelib/topolib"
)

var memBallast []byte

func main() {
	var storageDir = flag.String("walle.storage_dir", "", "Path where database and discovery data will be stored.")
	var host = flag.String(
		"walle.host", "",
		"Hostname that other servers can use to connect to this server. "+
			"If it isn't set, os.Hostname() will be used to determine it automatically.")
	var port = flag.String("walle.port", "", "Port to listen on.")
	var clusterURI = flag.String("walle.cluster_uri", "", "Cluster URI to join.")
	var bootstrapRootURI = flag.String(
		"walle.bootstrap_uri", "",
		"Bootstrap new deployment. Will exit once new bootstrapped storage is created.")

	// Tuning flags.
	var targetMemMB = flag.Int(
		"walle.target_mem_mb", 200, "Target maximum total memory usage. Recommended to be set at 60% of total memory size.")
	var maxLocalStreams = flag.Int(
		"walle.max_local_streams", 100, "Maximum number of streams that this server can handle.")
	var checkpointFrequency = flag.Duration(
		"walle.storage.checkpoint_frequency", 5*time.Minute,
		"Frequency at which internal WiredTiger storage engine checkpoints. "+
			"Longer checkpoint duration means longer recovery time if a crash occurs, "+
			"and also more disk space usage by WiredTiger log.")

	// Profiling/Debugging flags.
	var debugAddr = flag.String(
		"debug.addr", "",
		"<listen addr>:<port> to setup /debug HTTP endpoint on. For security it is best to limit listen "+
			"address to 127.0.0.1 (localhost) only.")
	flag.Parse()
	ctx, cancelAll := context.WithCancel(context.Background())

	err := datadog.ExporterGo(ctx)
	fatalOnErr(err)
	if *debugAddr != "" {
		err = serveDebug(*debugAddr)
		fatalOnErr(err)
	}

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
	// 60% goes to WT Cache. (non-GO memory)
	// 40% goes to (Static heap + Memory ballast) + GC overhead.
	debug.SetGCPercent(100) // GOGC=100, make it predictable and not tuneable.
	cacheSizeMB := *targetMemMB * 6 / 10
	ballastSize := *targetMemMB * 1024 * 1024 * 4 / 10 / 2

	zlog.Infof("initializing storage: %s...", dbPath)
	// Apply +/-10% jitter to checkpoint frequency to make sure nodes can't get into
	// unfortunate lock step with each other and always checkpoint at the same time.
	checkpointJitter := int64(*checkpointFrequency / 10)
	checkpointFreq := (*checkpointFrequency) + time.Duration(-checkpointJitter+mrand.Int63n(2*checkpointJitter))
	ss, err := storage.Init(dbPath, storage.InitOpts{
		Create:              true,
		CacheSizeMB:         cacheSizeMB,
		CheckpointFrequency: checkpointFreq,
		MaxLocalStreams:     *maxLocalStreams,
		LeakMemoryOnClose:   true,
	})
	fatalOnErr(err)
	zlog.Infof("initialized storage: %s", ss.ServerId())
	defer func() {
		<-ss.CloseC()
		zlog.Infof("storage closed and flushed")
	}()

	if *bootstrapRootURI != "" {
		err := server.BootstrapRoot(ss, *bootstrapRootURI, rootFile, serverInfo)
		fatalOnErr(err)
		zlog.Infof(
			"bootstrapped %s, server: %s - %s",
			*bootstrapRootURI, ss.ServerId(), serverInfo)
		ss.Close()
		return
	}

	// Ballast allocation must happen after storage is initialzied
	// but before anything else gets initialized. This way we get more correct
	// statically allocated heap size that storage module is using.
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	ballastSize -= int(memstats.HeapAlloc)
	if ballastSize > 0 {
		memBallast = make([]byte, ballastSize)
	}

	rootPb, err := wallelib.TopologyFromFile(rootFile)
	fatalOnErr(err)
	servingRootURI := *clusterURI == rootPb.RootUri
	rootD, err := wallelib.NewRootDiscovery(ctx, rootPb, !servingRootURI)
	fatalOnErr(err)
	rootCli := wallelib.NewClient(ctx, rootD)
	go watchTopologyAndSave(ctx, rootD, rootFile)
	var d wallelib.Discovery
	var c server.Client
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
	ws := server.New(ctx, ss, c, d, topoMgr)
	statsHandler := grpcstats.NewServer()
	s := grpc.NewServer(grpc.StatsHandler(statsHandler))
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
		s.GracefulStop()
	}()
	zlog.Infof("starting server on port:%s...", *port)
	err = s.Serve(l)
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
	topoMgr := topolib.NewClient(root)
	_, err := topoMgr.RegisterServer(ctx, &topomgr_pb.RegisterServerRequest{
		ClusterUri: clusterURI,
		ServerId:   serverId,
		ServerInfo: serverInfo,
	})
	return err
}

func serveDebug(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		err := server.ListenAndServe()
		fatalOnErr(err)
	}()
	return nil
}
