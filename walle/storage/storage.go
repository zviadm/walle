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
	c        *wt.Connection
	metaS    *wt.Session
	metaW    *wt.Mutator

	flushMX sync.Mutex
	flushS  *wt.Session

	maxLocalStreams int
	nLocalStreams   int
	// streamT contains topologies for all streams in a cluster.
	streamT sync.Map // stream_uri -> *walleapi.StreamTopology
	// streams contains only locally served streams.
	streams sync.Map // stream_uri -> Stream
}

var _ Storage = &storage{}

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
		r.streamT.Store(streamURI, topology)
	}
	r.streamT.Range(func(k, v interface{}) bool {
		streamURI := k.(string)
		topology := v.(*walleapi.StreamTopology)
		if !IsMember(topology, r.serverId) {
			return true
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
		return true
	})
	return r, nil
}

// Closing storage will leak underlying memory that is held by WiredTiger C library.
// Assumption is that, after closing storage, program will exit promptly.
func (m *storage) Close() {
	panic.OnErr(m.c.Close(wt.ConnCloseCfg{LeakMemory: wt.True}))
}

func (m *storage) ServerId() string {
	return m.serverId
}

func (m *storage) FlushSync() {
	m.flushMX.Lock()
	defer m.flushMX.Unlock()
	panic.OnErr(m.flushS.LogFlush(wt.SyncOn))
}

func (m *storage) Streams(localOnly bool) []string {
	r := make([]string, 0, m.maxLocalStreams)
	var mm *sync.Map
	if localOnly {
		mm = &m.streams
	} else {
		mm = &m.streamT
	}
	mm.Range(func(k, v interface{}) bool {
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

	tt, ok := m.streamT.Load(streamURI)
	if ok && tt.(*walleapi.StreamTopology).Version >= topology.Version {
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
	// It is very important to update topology in storage first to make sure
	// that if a server ACKs that it has received a particular version of topology it
	// won't lose it after a crash.
	panic.OnErr(m.metaS.TxBegin(wt.TxCfg{Sync: wt.True}))
	panic.OnErr(m.metaW.Insert([]byte(streamURI+sfxTopology), topologyB))
	panic.OnErr(m.metaS.TxCommit())
	m.streamT.Store(streamURI, topology)
	if ok {
		ss.setTopology(topology)
		if !isLocal {
			m.nLocalStreams -= 1
			m.streams.Delete(streamURI)
			ss.close()
		}
	} else if isLocal {
		ss = m.makeLocalStream(streamURI)
		ss.setTopology(topology)
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
