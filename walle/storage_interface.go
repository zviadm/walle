package walle

import (
	"github.com/zviadm/walle/proto/walleapi"
)

// Storage is expected to be thread-safe.
// Storage might cause panics for unrecoverable errors.
type Storage interface {
	ServerId() string
	Streams(localOnly bool) []string
	Stream(streamURI string, localOnly bool) (StreamStorage, bool)

	NewStream(streamURI string, topology *walleapi.StreamTopology)
	//RemoveStream(streamURI string)

	Close()
}

// StreamStorage is expected to be thread-safe.
type StreamStorage interface {
	StreamMetadata
	StreamData
}

// StreamMetadata is expected to be thread-safe.
type StreamMetadata interface {
	StreamURI() string
	WriterId() string // Perf sensitive. Needs to be in-memory.
	// Expected to have internal check to make sure stored writerId never decreases.
	UpdateWriterId(writerId string)

	Topology() *walleapi.StreamTopology
	// Expected to have internal check to make sure toplogy version never decreases.
	UpdateTopology(topology *walleapi.StreamTopology)
	IsLocal() bool
}

// StreamData is expected to be thread-safe.
type StreamData interface {
	LastEntries() []*walleapi.Entry
	ReadFrom(entryId int64) StreamCursor
	CommitEntry(entryId int64, entryMd5 []byte) (success bool)
	PutEntry(entry *walleapi.Entry, isCommitted bool) (success bool)

	CommittedEntryIds() (noGapCommittedId int64, committedId int64, notify <-chan struct{})
	UpdateNoGapCommittedId(entryId int64)
}

type StreamCursor interface {
	Next() (*walleapi.Entry, bool)
	Close()
}
