package walle

import (
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
)

// Storage is expected to be thread-safe.
// Storage might cause panics for unrecoverable errors.
type Storage interface {
	Streams() []string
	Stream(streamURI string) (StreamStorage, bool)
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

	Topology() *walle_pb.StreamTopology
}

// StreamData is expected to be thread-safe.
type StreamData interface {
	LastEntries() []*walleapi.Entry
	ReadFrom(entryId int64) StreamCursor
	CommitEntry(entryId int64, entryMd5 []byte) (success bool)
	PutEntry(entry *walleapi.Entry, isCommitted bool) (success bool)

	CommittedEntryIds() (noGapCommittedId int64, committedId int64)
	UpdateNoGapCommittedId(entryId int64)
}

type StreamCursor interface {
	Next() (*walleapi.Entry, bool)
	Close()
}
