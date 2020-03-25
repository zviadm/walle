package walle

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/wt"
)

type storage struct {
	serverId string
	c        *wt.Connection

	mx      sync.Mutex
	streams map[string]StreamStorage
}

var _ Storage = &storage{}

func StorageInit(dbPath string, createIfNotExists bool) (Storage, error) {
	return storageInitWithServerId(dbPath, createIfNotExists, "")
}

func storageInitWithServerId(dbPath string, createIfNotExists bool, serverId string) (Storage, error) {
	if createIfNotExists {
		if err := os.MkdirAll(dbPath, 0755); err != nil {
			return nil, err
		}
	}
	cfg := &wt.ConnectionConfig{
		Create:          wt.Bool(createIfNotExists),
		Log:             "enabled,compressor=snappy",
		TransactionSync: "enabled",
	}
	c, err := wt.Open(dbPath, cfg)
	if err != nil {
		return nil, err
	}
	s, err := c.OpenSession(nil)
	panicOnErr(err)
	defer s.Close()
	panicOnErr(
		s.Create(metadataDS, &wt.DataSourceConfig{BlockCompressor: "snappy"}))
	metaR, err := s.Scan(metadataDS)
	panicOnErr(err)
	defer metaR.Close()
	serverIdB, err := metaR.ReadUnsafeValue([]byte(glbServerId))
	if err != nil {
		if wt.ErrCode(err) != wt.ErrNotFound {
			panicOnErr(err)
		}
		if !createIfNotExists {
			return nil, errors.Errorf("serverId doesn't exist in the database: %s", dbPath)
		}
		metaW, err := s.Mutate(metadataDS, nil)
		panicOnErr(err)
		defer metaW.Close()
		if serverId != "" {
			serverIdB = []byte(serverId)
		} else {
			serverIdB = make([]byte, serverIdLen)
			rand.Read(serverIdB)
			glog.Infof(
				"initializing new database: %s, with serverId: %s...",
				dbPath, hex.EncodeToString(serverIdB))
		}
		panicOnErr(
			metaW.Insert([]byte(glbServerId), serverIdB))
	}
	r := &storage{
		serverId: string(serverIdB),
		c:        c,
		streams:  make(map[string]StreamStorage),
	}
	if serverId != "" && serverId != r.serverId {
		return nil, errors.Errorf("storage already has different serverId: %s vs %s", r.serverId, serverId)
	}
	glog.Infof("storage: %s", hex.EncodeToString(serverIdB))

	streamURIs := make(map[string]struct{})
	panicOnErr(metaR.Reset())
	for {
		if err := metaR.Next(); err != nil {
			if wt.ErrCode(err) != wt.ErrNotFound {
				panicOnErr(err)
			}
			break
		}
		metaKey, err := metaR.UnsafeKey()
		panicOnErr(err)
		if metaKey[0] != '/' {
			continue
		}
		streamURI := strings.Split(string(metaKey), ":")[0]
		streamURIs[streamURI] = struct{}{}
	}
	for streamURI := range streamURIs {
		sess, err := c.OpenSession(nil)
		panicOnErr(err)
		r.streams[streamURI] = openStreamStorage(r.serverId, streamURI, sess)
		glog.Infof("stream: %s (isLocal? %t)", streamURI, r.streams[streamURI].IsLocal())
	}
	return r, nil
}

func (m *storage) Close() {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.c.Close()
}

func (m *storage) ServerId() string {
	return m.serverId
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

func (m *storage) Stream(streamURI string, localOnly bool) (StreamStorage, bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	r, ok := m.streams[streamURI]
	if ok && localOnly && !r.IsLocal() {
		return nil, false
	}
	return r, ok
}

func (m *storage) NewStream(streamURI string, t *walleapi.StreamTopology) StreamStorage {
	sess, err := m.c.OpenSession(nil)
	panicOnErr(err)
	s := createStreamStorage(m.serverId, streamURI, t, sess)
	m.mx.Lock()
	defer m.mx.Unlock()
	m.streams[streamURI] = s
	return s
}
