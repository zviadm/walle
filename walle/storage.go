package walle

import (
	"crypto/rand"
	"os"
	"sync"

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
	c, err := wt.Open(dbPath, &wt.ConnectionConfig{
		Create: &createIfNotExists,
		Log:    "enabled,compressor=snappy",
	})
	if err != nil {
		return nil, err
	}
	s, err := c.OpenSession(nil)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	if err := s.Create(metadataDS, &wt.DataSourceConfig{BlockCompressor: "snappy"}); err != nil {
		return nil, err
	}
	metaR, err := s.Scan(metadataDS)
	if err != nil {
		return nil, err
	}
	defer metaR.Close()
	serverIdB, err := metaR.ReadUnsafeValue([]byte(glbServerId))
	if err != nil {
		if wt.ErrCode(err) != wt.ErrNotFound {
			return nil, err
		}
		if !createIfNotExists {
			return nil, errors.Errorf("serverId doesn't exist in the database: %v", dbPath)
		}
		metaW, err := s.Mutate(metadataDS, nil)
		if err != nil {
			return nil, err
		}
		defer metaW.Close()
		if serverId != "" {
			serverIdB = []byte(serverId)
		} else {
			serverIdB = make([]byte, serverIdLen)
			rand.Read(serverIdB)
		}
		if err := metaW.Insert([]byte(glbServerId), serverIdB); err != nil {
			return nil, err
		}
	}
	r := &storage{
		serverId: string(serverIdB),
		c:        c,
		streams:  make(map[string]StreamStorage),
	}
	if serverId != "" && serverId != r.serverId {
		return nil, errors.Errorf("storage already has different serverId: %v vs %v", r.serverId, serverId)
	}
	// TODO(zviad): Load all streams that already exist in metadata.
	return r, nil
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

func (m *storage) NewStream(streamURI string, t *walleapi.StreamTopology) {
	sess, err := m.c.OpenSession(nil)
	panicOnErr(err)
	s := createStreamStorage(m.serverId, streamURI, t, sess)
	m.mx.Lock()
	defer m.mx.Unlock()
	m.streams[streamURI] = s
}
