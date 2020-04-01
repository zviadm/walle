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
	flushS   *wt.Session
	metaS    *wt.Session
	metaW    *wt.Mutator

	mx      sync.Mutex
	streamT map[string]*walleapi.StreamTopology // exists for all streamURIs
	streams map[string]Stream                   // exists only for local streams
}

var _ Storage = &storage{}

type InitOpts struct {
	Create          bool   // create database if it doesn't exist.
	ServerId        string // use provided serverId. only needed in testing.
	MaxLocalStreams int    // maximum number of local streams supported.
}

func Init(dbPath string, opts InitOpts) (Storage, error) {
	if opts.Create {
		if err := os.MkdirAll(dbPath, 0755); err != nil {
			return nil, err
		}
	}
	if opts.MaxLocalStreams == 0 {
		opts.MaxLocalStreams = 100
	}
	cfg := &wt.ConnectionConfig{
		Create:     wt.Bool(opts.Create),
		Log:        "enabled,compressor=snappy",
		SessionMax: opts.MaxLocalStreams*2 + 2,
	}
	c, err := wt.Open(dbPath, cfg)
	if err != nil {
		return nil, err
	}
	metaS, err := c.OpenSession(nil)
	panic.OnErr(err)
	panic.OnErr(metaS.Create(metadataDS, &wt.DataSourceConfig{BlockCompressor: "snappy"}))
	metaR, err := metaS.Scan(metadataDS)
	panic.OnErr(err)
	defer metaR.Close()
	metaW, err := metaS.Mutate(metadataDS, nil)
	panic.OnErr(err)
	serverIdB, err := metaR.ReadUnsafeValue([]byte(glbServerId))
	if err != nil {
		if wt.ErrCode(err) != wt.ErrNotFound {
			panic.OnErr(err)
		}
		if !opts.Create {
			return nil, errors.Errorf("serverId doesn't exist in the database: %s", dbPath)
		}
		if opts.ServerId != "" {
			serverIdB = []byte(opts.ServerId)
		} else {
			serverIdB = make([]byte, serverIdLen)
			rand.Read(serverIdB)
			zlog.Infof(
				"initializing new database: %s, with serverId: %s...",
				dbPath, hex.EncodeToString(serverIdB))
		}
		panic.OnErr(
			metaW.Insert([]byte(glbServerId), serverIdB))
	}
	flushS, err := c.OpenSession(nil)
	panic.OnErr(err)
	r := &storage{
		serverId: string(serverIdB),
		c:        c,
		flushS:   flushS,
		metaS:    metaS,
		metaW:    metaW,
		streams:  make(map[string]Stream),
	}
	if opts.ServerId != "" && opts.ServerId != r.serverId {
		return nil, errors.Errorf(
			"storage already has different serverId: %s vs %s", r.serverId, opts.ServerId)
	}
	zlog.Infof("storage: %s", hex.EncodeToString(serverIdB))

	r.streamT = make(map[string]*walleapi.StreamTopology)
	panic.OnErr(metaR.Reset())
	for {
		if err := metaR.Next(); err != nil {
			if wt.ErrCode(err) != wt.ErrNotFound {
				panic.OnErr(err)
			}
			break
		}
		metaKeyB, err := metaR.UnsafeKey()
		panic.OnErr(err)
		metaKey := string(metaKeyB)
		if !strings.HasSuffix(metaKey, sfxTopology) {
			continue
		}
		v, err := metaR.UnsafeValue()
		panic.OnErr(err)
		topology := &walleapi.StreamTopology{}
		panic.OnErr(topology.Unmarshal(v))

		streamURI := strings.Split(metaKey, ":")[0]
		r.streamT[streamURI] = topology
	}
	for streamURI, topology := range r.streamT {
		if !isLocalStream(r.serverId, topology) {
			continue
		}
		sess, err := c.OpenSession(nil)
		panic.OnErr(err)
		sessRO, err := c.OpenSession(nil)
		panic.OnErr(err)
		r.streams[streamURI] = openStreamStorage(r.serverId, streamURI, sess, sessRO)
		r.streams[streamURI].setTopology(topology)
		zlog.Infof("stream (local): %s", streamURI)
	}
	return r, nil
}

func (m *storage) Close() {
	m.mx.Lock()
	defer m.mx.Unlock()
	panic.OnErr(m.c.Close())
}

func (m *storage) ServerId() string {
	return m.serverId
}

func (m *storage) FlushSync() {
	m.mx.Lock()
	defer m.mx.Unlock()
	panic.OnErr(m.flushS.LogFlush(wt.SyncOn))
}

func (m *storage) Streams(localOnly bool) []string {
	m.mx.Lock()
	defer m.mx.Unlock()
	r := make([]string, 0, len(m.streamT))
	for streamURI := range m.streamT {
		_, ok := m.streams[streamURI]
		if localOnly && !ok {
			continue
		}
		r = append(r, streamURI)
	}
	return r
}

func (m *storage) Stream(streamURI string) (Stream, bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	r, ok := m.streams[streamURI]
	return r, ok
}

func (m *storage) Update(
	streamURI string, topology *walleapi.StreamTopology) error {
	if m.streamT[streamURI].GetVersion() >= topology.Version {
		return nil
	}

	var ss Stream = nil
	topologyB, err := topology.Marshal()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			return
		}
		panic.OnErr(m.metaW.Insert([]byte(streamURI+sfxTopology), topologyB))
		m.mx.Lock()
		defer m.mx.Unlock()
		m.streamT[streamURI] = topology
		if ss == nil {
			delete(m.streams, streamURI)
		} else {
			m.streams[streamURI] = ss
			ss.setTopology(topology)
		}
	}()
	isLocal := isLocalStream(m.serverId, topology)
	var ok bool
	ss, ok = m.Stream(streamURI)
	if ok == isLocal {
		return nil
	} else if !isLocal {
		ss.close()
		ss = nil
		return nil
	} else {
		var err error
		ss, err = m.makeLocalStream(streamURI)
		return err
	}
}

func (m *storage) makeLocalStream(streamURI string) (Stream, error) {
	sess, err := m.c.OpenSession(nil)
	if err != nil {
		return nil, err
	}
	sessRO, err := m.c.OpenSession(nil)
	if err != nil {
		panic.OnErr(sess.Close()) // close successfully opened session.
		return nil, err
	}
	s := createStreamStorage(m.serverId, streamURI, sess, sessRO)
	return s, nil
}
