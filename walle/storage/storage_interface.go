package storage

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

	NewStream(streamURI string, topology *walleapi.StreamTopology) StreamStorage
	//RemoveStream(streamURI string)

	// Forces Flushes
	FlushSync()

	Close()
}

// StreamStorage is expected to be thread-safe.
// Note that StreamMetadata calls are all synced automatically in the transaction log,
// however StreamStorage calls aren't and require explicit call to FlushSync method to
// guarantee durability.
type StreamStorage interface {
	StreamMetadata

	CommittedEntryIds() (noGapCommittedId int64, committedId int64, notify <-chan struct{})
	TailEntryId() (entryId int64, notify <-chan struct{})
	// Returns last committed entry and all the following not-yet committed entries.
	LastEntries() []*walleapi.Entry
	// Returns cursor to read committed entries starting at entryId. Cursor should be used
	// as soon as possible and shouldn't be held open for too long (i.e. while waiting on some
	// network i/o).
	ReadFrom(entryId int64) StreamCursor

	UpdateNoGapCommittedId(entryId int64)
	CommitEntry(entryId int64, entryMd5 []byte) (success bool)
	PutEntry(entry *walleapi.Entry, isCommitted bool) (success bool)
}

// StreamMetadata is expected to be thread-safe.
type StreamMetadata interface {
	StreamURI() string
	// WriterId() string // Perf sensitive. Needs to be in-memory.
	WriterInfo() (writerId WriterId, writerAddr string, lease time.Duration, remainingLease time.Duration)
	// Update call is expected to have an internal check to make sure stored writerId never decreases.
	UpdateWriter(writerId WriterId, writerAddr string, lease time.Duration) (success bool, remainingLease time.Duration)
	RenewLease(writerId WriterId)

	Topology() *walleapi.StreamTopology
	// Update call is expected to have an internal check to make sure toplogy version never decreases.
	UpdateTopology(topology *walleapi.StreamTopology)
	IsLocal() bool
}

// StreamCursor can be used to read entries from StreamStorage. It is safe to call Close() on
// already closed cursor. Cursor is also automatically closed once Next() exhausts all
// committed entries.
type StreamCursor interface {
	Next() (*walleapi.Entry, bool)
	Close()
}
