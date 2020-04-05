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
	// Updates or sets new topology for given streamURI. Can return an error if
	// local resoures are exhausted and storage can no longer handle adding more streams.
	// This call is not thread safe. There must be only one thread that makes Update calls.
	Update(streamURI string, topology *walleapi.StreamTopology) error
	Streams(localOnly bool) []string
	Stream(streamURI string) (Stream, bool)

	FlushSync() // Forces Flush
	Close()
}

// Stream is expected to be thread-safe.
// Note that StreamMetadata calls are all synced automatically in the transaction log,
// however Stream calls aren't and require explicit call to FlushSync method to
// guarantee durability.
type Stream interface {
	Metadata
	close()
	IsClosed() bool

	// Returns EntryId for maximum committed entry.
	CommittedEntryId() (committedId int64, notify <-chan struct{})
	// Returns EntryId for maximum entry that has been stored. May not be committed.
	TailEntryId() (entryId int64, notify <-chan struct{})
	// Returns range that covers all potentially missing entries: [startId...endId)
	// If startId >= endId, there are no missing entries.
	GapRange() (startId int64, endId int64)
	// Returns last committed entry and all the following not-yet committed entries.
	TailEntries() ([]*walleapi.Entry, error)
	// Returns cursor to read committed entries starting at entryId.
	ReadFrom(entryId int64) (Cursor, error)

	CommitEntry(entryId int64, entryMd5 []byte) (success bool)
	PutEntry(entry *walleapi.Entry, isCommitted bool) (success bool)
	UpdateGapStart(entryId int64)
}

// Metadata is expected to be thread-safe.
type Metadata interface {
	StreamURI() string
	// WriterId() string // Perf sensitive. Needs to be in-memory.
	WriterInfo() (writerId WriterId, writerAddr string, lease time.Duration, remainingLease time.Duration)
	// Update call is expected to have an internal check to make sure stored writerId never decreases.
	UpdateWriter(writerId WriterId, writerAddr string, lease time.Duration) (success bool, remainingLease time.Duration)
	RenewLease(writerId WriterId, extraBuffer time.Duration) bool

	setTopology(topology *walleapi.StreamTopology)
	Topology() *walleapi.StreamTopology
}

// Cursor can be used to read entries from Stream. It is safe to call Close() on
// already closed cursor. Cursor is also automatically closed once Next() exhausts all
// committed entries.
type Cursor interface {
	Next() (*walleapi.Entry, bool)
	Skip() (int64, bool)
	Close()
}
