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

	mx      sync.Mutex
	streams map[string]Stream
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
		SessionMax: opts.MaxLocalStreams*2 + 1,
	}
	c, err := wt.Open(dbPath, cfg)
	if err != nil {
		return nil, err
	}
	s, err := c.OpenSession(nil)
	panic.OnErr(err)
	defer s.Close()
	panic.OnErr(
		s.Create(metadataDS, &wt.DataSourceConfig{BlockCompressor: "snappy"}))
	metaR, err := s.Scan(metadataDS)
	panic.OnErr(err)
	defer metaR.Close()
	serverIdB, err := metaR.ReadUnsafeValue([]byte(glbServerId))
	if err != nil {
		if wt.ErrCode(err) != wt.ErrNotFound {
			panic.OnErr(err)
		}
		if !opts.Create {
			return nil, errors.Errorf("serverId doesn't exist in the database: %s", dbPath)
		}
		metaW, err := s.Mutate(metadataDS, nil)
		panic.OnErr(err)
		defer metaW.Close()
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
		streams:  make(map[string]Stream),
	}
	if opts.ServerId != "" && opts.ServerId != r.serverId {
		return nil, errors.Errorf(
			"storage already has different serverId: %s vs %s", r.serverId, opts.ServerId)
	}
	zlog.Infof("storage: %s", hex.EncodeToString(serverIdB))

	streamURIs := make(map[string]struct{})
	panic.OnErr(metaR.Reset())
	for {
		if err := metaR.Next(); err != nil {
			if wt.ErrCode(err) != wt.ErrNotFound {
				panic.OnErr(err)
			}
			break
		}
		metaKey, err := metaR.UnsafeKey()
		panic.OnErr(err)
		if metaKey[0] != '/' {
			continue
		}
		streamURI := strings.Split(string(metaKey), ":")[0]
		streamURIs[streamURI] = struct{}{}
	}
	for streamURI := range streamURIs {
		sess, err := c.OpenSession(nil)
		panic.OnErr(err)
		sessRO, err := c.OpenSession(nil)
		panic.OnErr(err)
		r.streams[streamURI] = openStreamStorage(r.serverId, streamURI, sess, sessRO)
		zlog.Infof("stream: %s (isLocal? %t)", streamURI, r.streams[streamURI].IsLocal())
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
	r := make([]string, 0, len(m.streams))
	for streamURI, s := range m.streams {
		if localOnly && !s.IsLocal() {
			continue
		}
		r = append(r, streamURI)
	}
	return r
}

func (m *storage) Stream(streamURI string, localOnly bool) (Stream, bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	r, ok := m.streams[streamURI]
	if ok && localOnly && !r.IsLocal() {
		return nil, false
	}
	return r, ok
}

func (m *storage) NewStream(
	streamURI string, t *walleapi.StreamTopology) (Stream, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if _, ok := m.streams[streamURI]; ok {
		return nil, errors.Errorf("ERR_FATAL; stream %s already exists!", streamURI)
	}
	sess, err := m.c.OpenSession(nil)
	if err != nil {
		return nil, err
	}
	sessRO, err := m.c.OpenSession(nil)
	if err != nil {
		sess.Close() // close successfully opened session.
		return nil, err
	}
	s := createStreamStorage(m.serverId, streamURI, t, sess, sessRO)
	m.streams[streamURI] = s
	return s, nil
}
