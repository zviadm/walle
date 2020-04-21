package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/wt"
	"github.com/zviadm/zlog"
)

type storage struct {
	serverId  string
	closeC    chan struct{}
	fastClose bool
	c         *wt.Connection
	metaS     *wt.Session
	metaC     *wt.Cursor

	backgroundWG sync.WaitGroup
	cancelBG     context.CancelFunc

	flushMX   sync.Mutex
	flushQ    chan struct{}
	flushDone chan struct{}

	trimQ chan struct{}

	maxLocalStreams int
	// streams contains only locally served streams.
	streams sync.Map // stream_uri -> Stream

	// streamT and nLocalStreams are only acccessed from a single thread.
	streamT       map[string]*walleapi.StreamTopology // contains topologies for all streams in a cluster.
	nLocalStreams int
}

// InitOpts contains initialization options for Init call.
type InitOpts struct {
	Create              bool   // create database if it doesn't exist.
	ServerId            string // use provided serverId. only needed in testing.
	CacheSizeMB         int
	CheckpointFrequency time.Duration
	MaxLocalStreams     int // maximum number of local streams supported.
	// If True, will leak memory when closing. This speed up close, and can be safe
	// if process is about to exit anyways.
	LeakMemoryOnClose bool
}

// Init call opens or creates WALLE database.
func Init(dbPath string, opts InitOpts) (Storage, error) {
	if opts.Create {
		if err := os.MkdirAll(dbPath, 0755); err != nil {
			return nil, err
		}
	}
	if opts.MaxLocalStreams == 0 {
		opts.MaxLocalStreams = 100
	}
	cfg := wt.ConnCfg{
		Create:          wt.Bool(opts.Create),
		Log:             "enabled,compressor=snappy",
		SessionMax:      opts.MaxLocalStreams*3 + 30, // +30 is as a buffer since WT internal threads also use sessions.
		CacheSize:       opts.CacheSizeMB * 1024 * 1024,
		TransactionSync: "enabled=false", // Flushes happen manually by `flushLoop` go routine.

		// Statistics:    []wt.Statistics{wt.StatsFast},
		// StatisticsLog: "wait=30,source=table:",
	}
	if opts.CheckpointFrequency > 0 {
		cfg.Checkpoint = "wait=" + strconv.Itoa(int(opts.CheckpointFrequency.Seconds()))
	}
	c, err := wt.Open(dbPath, cfg)
	if err != nil {
		return nil, err
	}
	metaS, err := c.OpenSession()
	panic.OnErr(err)
	// Metadata table is expected to be very small with very low traffic,
	// thus there is no need for compression or any other options on it.
	metadataCfg := wt.DataSourceCfg{}
	panic.OnErr(metaS.Create(metadataDS, metadataCfg))
	metaC, err := metaS.OpenCursor(metadataDS)
	panic.OnErr(err)
	defer func() { panic.OnErr(metaC.Reset()) }()

	var serverId string
	serverIdB, err := metaC.ReadValue([]byte(glbServerId))
	if err != nil {
		if wt.ErrCode(err) != wt.ErrNotFound {
			panic.OnErr(err)
		}
		if !opts.Create {
			return nil, errors.Errorf("serverId doesn't exist in the database: %s", dbPath)
		}
		if opts.ServerId != "" {
			serverId = opts.ServerId
		} else {
			serverIdB = make([]byte, 8)
			_, err := rand.Read(serverIdB)
			if err != nil {
				return nil, err
			}
			serverId = hex.EncodeToString(serverIdB)
		}
		panic.OnErr(metaC.Insert([]byte(glbServerId), []byte(serverId)))
	} else {
		serverId = string(serverIdB)
	}
	if opts.ServerId != "" && opts.ServerId != serverId {
		return nil, errors.Errorf(
			"storage already has different serverId: %s vs %s", serverId, opts.ServerId)
	}

	r := &storage{
		serverId:        serverId,
		closeC:          make(chan struct{}),
		fastClose:       opts.LeakMemoryOnClose,
		c:               c,
		metaS:           metaS,
		metaC:           metaC,
		maxLocalStreams: opts.MaxLocalStreams,

		flushQ:    make(chan struct{}, 1),
		flushDone: make(chan struct{}),

		trimQ: make(chan struct{}, 1),
	}
	if opts.ServerId != "" && opts.ServerId != r.serverId {
		return nil, errors.Errorf(
			"storage already has different serverId: %s vs %s", r.serverId, opts.ServerId)
	}

	r.streamT = make(map[string]*walleapi.StreamTopology)
	panic.OnErr(metaC.Reset())
	for {
		if err := metaC.Next(); err != nil {
			if wt.ErrCode(err) != wt.ErrNotFound {
				panic.OnErr(err)
			}
			break
		}
		metaKeyB, err := metaC.Key()
		panic.OnErr(err)
		metaKey := string(metaKeyB)
		if !strings.HasSuffix(metaKey, sfxTopology) {
			continue
		}
		v, err := metaC.Value()
		panic.OnErr(err)
		topology := &walleapi.StreamTopology{}
		panic.OnErr(topology.Unmarshal(v))

		streamURI := strings.Split(metaKey, ":")[0]
		r.streamT[streamURI] = topology
	}
	for streamURI, topology := range r.streamT {
		if !IsMember(topology, r.serverId) {
			continue
		}
		sess, err := c.OpenSession()
		panic.OnErr(err)
		sessRO, err := c.OpenSession()
		panic.OnErr(err)
		sessFill, err := c.OpenSession()
		panic.OnErr(err)
		ss := openStreamStorage(r.serverId, streamURI, sess, sessRO, sessFill)
		ss.setTopology(topology)
		r.nLocalStreams += 1
		r.streams.Store(streamURI, ss)
		zlog.Infof("stream (local): %s %s (v: %d)", streamURI, topology.ServerIds, topology.Version)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r.cancelBG = cancel
	r.backgroundWG.Add(2)
	go r.flushLoop(ctx)
	go r.trimLoop(ctx)
	return r, nil
}

func (m *storage) ServerId() string {
	return m.serverId
}

func (m *storage) flushLoop(ctx context.Context) {
	defer m.backgroundWG.Done()
	s, err := m.c.OpenSession()
	panic.OnErr(err)
	for {
		select {
		case <-ctx.Done():
			panic.OnErr(s.LogFlush(wt.SyncOn))
			return
		case <-m.flushQ:
		}
		m.flushMX.Lock()
		flushDone := m.flushDone
		m.flushDone = make(chan struct{})
		m.flushMX.Unlock()

		panic.OnErr(s.LogFlush(wt.SyncOn))
		close(flushDone)
	}
}

func (m *storage) Flush(ctx context.Context) error {
	m.flushMX.Lock()
	done := m.flushDone
	m.flushMX.Unlock()
	select {
	case m.flushQ <- struct{}{}:
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	return nil
}

func (m *storage) LocalStreams() []string {
	r := make([]string, 0, m.maxLocalStreams)
	m.streams.Range(func(k, v interface{}) bool {
		r = append(r, k.(string))
		return true
	})
	return r
}

func (m *storage) Stream(streamURI string) (Stream, bool) {
	v, ok := m.streams.Load(streamURI)
	if !ok {
		return nil, false
	}
	return v.(Stream), true
}

// UpstertStream call is NOT thread-safe. Must be called from a single
// thread only.
func (m *storage) CrUpdateStream(
	streamURI string, topology *walleapi.StreamTopology) error {

	tt, ok := m.streamT[streamURI]
	if ok && tt.Version >= topology.Version {
		return nil
	}
	topologyB, err := topology.Marshal()
	if err != nil {
		return err
	}
	isLocal := IsMember(topology, m.serverId)
	ss, ok := m.Stream(streamURI)
	if isLocal && !ok && m.nLocalStreams >= m.maxLocalStreams {
		return errors.Errorf("max streams reached: %d", m.maxLocalStreams)
	}
	if isLocal && !ok {
		ss = m.makeLocalStream(streamURI)
	}
	// It is very important to update topology in storage first before in m.streamT or
	// before in m.streams map, to make sure server doesn't ACK that it has received particular
	// version of topology that it might lose in a crash.
	panic.OnErr(m.metaS.TxBegin(wt.TxCfg{Sync: wt.True}))
	panic.OnErr(m.metaC.Insert([]byte(streamURI+sfxTopology), topologyB))
	panic.OnErr(m.metaS.TxCommit())
	notifyTrimLoop := m.streamT[streamURI].GetFirstEntryId() < topology.FirstEntryId
	m.streamT[streamURI] = topology
	if ss != nil {
		ss.setTopology(topology)
		if notifyTrimLoop {
			select {
			case m.trimQ <- struct{}{}:
			default:
			}
		}
	}
	if ok && !isLocal {
		m.nLocalStreams -= 1
		m.streams.Delete(streamURI)
		ss.close()
	} else if !ok && isLocal {
		m.nLocalStreams += 1
		m.streams.Store(streamURI, ss)
	}
	return nil
}

func (m *storage) makeLocalStream(streamURI string) Stream {
	sess, err := m.c.OpenSession()
	panic.OnErr(err)
	sessRO, err := m.c.OpenSession()
	panic.OnErr(err)
	sessFill, err := m.c.OpenSession()
	panic.OnErr(err)
	s := createStreamStorage(m.serverId, streamURI, sess, sessRO, sessFill)
	return s
}

func (m *storage) Close() {
	m.streams.Range(func(k, v interface{}) bool {
		v.(Stream).close()
		return true
	})
	m.cancelBG()
	m.backgroundWG.Wait()
	panic.OnErr(m.c.Close(wt.ConnCloseCfg{LeakMemory: wt.Bool(m.fastClose)}))
	close(m.closeC)
}

func (m *storage) CloseC() <-chan struct{} {
	return m.closeC
}
