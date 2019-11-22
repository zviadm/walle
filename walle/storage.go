package walle

import (
	walle_pb "github.com/zviadm/walle/proto/walle"
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
	// Perf sensitive. Needs to be in-memory.
	WriterId() string
	// Expected to have internal check to make sure stored writerId never decreases.
	UpdateWriterId(writerId string)

	Topology() *walle_pb.StreamTopology
}

// StreamData is expected to be thread-safe.
type StreamData interface {
	LastEntry(includeUncommitted bool) []*walle_pb.Entry
	CommitEntry(entryId int64, entryMd5 []byte) bool
	PutEntry(entry *walle_pb.Entry, isCommitted bool) bool
}
