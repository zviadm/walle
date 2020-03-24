package walle

import (
	"time"

	"github.com/zviadm/walle/proto/walleapi"
)

// Storage is expected to be thread-safe.
// Storage might cause panics for unrecoverable errors due to bad disk i/o
// or due to on disk data corruption issues.
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
	// Returns last committed entry and all the following not-yet committed entries.
	LastEntries() []*walleapi.Entry
	// Returns cursor to read committed entries starting at entryId.
	ReadFrom(entryId int64) StreamCursor
	CommitEntry(entryId int64, entryMd5 []byte) (success bool)
	PutEntry(entry *walleapi.Entry, isCommitted bool) (success bool)
}

// StreamMetadata is expected to be thread-safe.
type StreamMetadata interface {
	StreamURI() string
	// WriterId() string // Perf sensitive. Needs to be in-memory.
	WriterInfo() (writerId string, writerAddr string, lease time.Duration)
	// Update call is expected to have an internal check to make sure stored writerId never decreases.
	UpdateWriter(writerId string, writerAddr string, lease time.Duration)

	Topology() *walleapi.StreamTopology
	// Update call is expected to have an internal check to make sure toplogy version never decreases.
	UpdateTopology(topology *walleapi.StreamTopology)
	IsLocal() bool

	CommittedEntryIds() (noGapCommittedId int64, committedId int64, notify <-chan struct{})
	UpdateNoGapCommittedId(entryId int64)
}

// StreamCursor can be used to read entries from StreamStorage. It is safe to call Close() on
// already closed cursor. Cursor is also automatically closed once Next() exhausts all
// committed entries.
type StreamCursor interface {
	Next() (*walleapi.Entry, bool)
	Close()
}
