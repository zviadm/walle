package walle

import (
	"crypto/md5"
	"sync"

	"github.com/zviadm/walle/proto/walleapi"
)

type mockStorage struct {
	serverId string
	mx       sync.Mutex
	streams  map[string]*mockStream
}

var _ Storage = &mockStorage{}

// TODO(zviad): This will be removed.
func NewMockStorage() *mockStorage {
	return newMockStorage("")
}

func newMockStorage(serverId string) *mockStorage {
	return &mockStorage{
		serverId: serverId,
		streams:  make(map[string]*mockStream),
	}
}

func (m *mockStorage) ServerId() string {
	return m.serverId
}

func (m *mockStorage) Streams(localOnly bool) []string {
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

func (m *mockStorage) Stream(streamURI string, localOnly bool) (StreamStorage, bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	r, ok := m.streams[streamURI]
	if ok && localOnly && !r.IsLocal() {
		return nil, false
	}
	return r, ok
}

func (m *mockStorage) NewStream(streamURI string, t *walleapi.StreamTopology) {
	s := &mockStream{
		serverId:        m.serverId,
		streamURI:       streamURI,
		entries:         []*walleapi.Entry{&walleapi.Entry{ChecksumMd5: make([]byte, md5.Size)}},
		committedNotify: make(chan struct{}),
	}
	s.UpdateTopology(t)

	m.mx.Lock()
	defer m.mx.Unlock()
	m.streams[streamURI] = s
}
