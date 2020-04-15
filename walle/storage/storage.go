package storage

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/wt"
	"github.com/zviadm/zlog"
)

type storage struct {
	serverId string
	closeC   chan struct{}
	c        *wt.Connection
	metaS    *wt.Session
	metaW    *wt.Mutator

	flushMX sync.Mutex
	flushS  *wt.Session

	maxLocalStreams int
	// streams contains only locally served streams.
	streams sync.Map // stream_uri -> Stream

	// streamT and nLocalStreams are only acccessed from a single thread.
	streamT       map[string]*walleapi.StreamTopology // contains topologies for all streams in a cluster.
	nLocalStreams int
}

// InitOpts contains initialization options for Init call.
type InitOpts struct {
	Create          bool   // create database if it doesn't exist.
	ServerId        string // use provided serverId. only needed in testing.
	CacheSizeMB     int
	MaxLocalStreams int // maximum number of local streams supported.
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
		Create:     wt.Bool(opts.Create),
		Log:        "enabled,compressor=snappy",
		SessionMax: opts.MaxLocalStreams*3 + 30, // +30 is as a buffer since WT internal threads also use sessions.
		CacheSize:  opts.CacheSizeMB * 1024 * 1024,

		// Statistics:    []wt.Statistics{wt.StatsFast},
		// StatisticsLog: "wait=30,source=table:",
	}
	c, err := wt.Open(dbPath, cfg)
	if err != nil {
		return nil, err
	}
	metaS, err := c.OpenSession()
	panic.OnErr(err)
	panic.OnErr(metaS.Create(metadataDS, wt.DataSourceCfg{BlockCompressor: "snappy"}))
	metaR, err := metaS.Scan(metadataDS)
	panic.OnErr(err)
	defer metaR.Close()
	metaW, err := metaS.Mutate(metadataDS)
	panic.OnErr(err)

	var serverId string
	serverIdB, err := metaR.ReadValue([]byte(glbServerId))
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
		panic.OnErr(metaW.Insert([]byte(glbServerId), []byte(serverId)))
	} else {
		serverId = string(serverIdB)
	}
	if opts.ServerId != "" && opts.ServerId != serverId {
		return nil, errors.Errorf(
			"storage already has different serverId: %s vs %s", serverId, opts.ServerId)
	}

	flushS, err := c.OpenSession()
	panic.OnErr(err)
	r := &storage{
		serverId:        serverId,
		closeC:          make(chan struct{}),
		c:               c,
		flushS:          flushS,
		metaS:           metaS,
		metaW:           metaW,
		maxLocalStreams: opts.MaxLocalStreams,
	}
	if opts.ServerId != "" && opts.ServerId != r.serverId {
		return nil, errors.Errorf(
			"storage already has different serverId: %s vs %s", r.serverId, opts.ServerId)
	}

	r.streamT = make(map[string]*walleapi.StreamTopology)
	panic.OnErr(metaR.Reset())
	for {
		if err := metaR.Next(); err != nil {
			if wt.ErrCode(err) != wt.ErrNotFound {
				panic.OnErr(err)
			}
			break
		}
		metaKeyB, err := metaR.Key()
		panic.OnErr(err)
		metaKey := string(metaKeyB)
		if !strings.HasSuffix(metaKey, sfxTopology) {
			continue
		}
		v, err := metaR.Value()
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
	return r, nil
}

func (m *storage) ServerId() string {
	return m.serverId
}

func (m *storage) FlushSync() {
	m.flushMX.Lock()
	defer m.flushMX.Unlock()
	panic.OnErr(m.flushS.LogFlush(wt.SyncOn))
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
func (m *storage) UpsertStream(
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
	panic.OnErr(m.metaW.Insert([]byte(streamURI+sfxTopology), topologyB))
	panic.OnErr(m.metaS.TxCommit())
	m.streamT[streamURI] = topology
	if ss != nil {
		ss.setTopology(topology)
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

// Closing storage will leak underlying memory that is held by WiredTiger C library.
// Assumption is that, after closing storage, program will exit promptly.
// Close is not thread safe, and should be called by same thread that calls UpsertStream
// method.
func (m *storage) Close() {
	m.streams.Range(func(k, v interface{}) bool {
		v.(Stream).close()
		return true
	})
	m.FlushSync()
	panic.OnErr(m.c.Close(wt.ConnCloseCfg{LeakMemory: wt.True}))
	close(m.closeC)
}

func (m *storage) CloseC() <-chan struct{} {
	return m.closeC
}
