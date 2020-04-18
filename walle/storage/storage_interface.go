package storage

import (
	"context"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
)

// Storage is expected to be thread-safe.
// Storage might cause panics for unrecoverable errors due to bad disk i/o
// or due to on disk data corruption issues.
type Storage interface {
	ServerId() string
	// CrUpdateStream creates or updates stream with given topology. Can return an error if
	// local resoures are exhausted and storage can no longer handle adding more streams.
	// This call is not thread safe. There must be only one thread that makes CrUpdate calls.
	CrUpdateStream(streamURI string, topology *walleapi.StreamTopology) error
	// Close closes storage. Closing can leak underlying memory that is held by WiredTiger
	// C library. Assumption is that, after closing storage, program will exit promptly.
	// Close is not thread safe, and should be called by same thread that calls CrUpdateStream
	// method.
	Close()
	// CloseC returns channel that will be closed once storage is closed.
	CloseC() <-chan struct{}

	LocalStreams() []string
	Stream(streamURI string) (Stream, bool)

	// Flush queues and waits for storage write ahead log flush to succeed. Can return
	// an error if context expires.
	Flush(ctx context.Context) error
}

// Stream is expected to be thread-safe.
// Note that StreamMetadata calls are all synced automatically in the transaction log,
// however Stream calls aren't and require explicit call to FlushSync method to
// guarantee durability.
type Stream interface {
	Metadata
	close()
	IsClosed() bool

	// CommitNotify returns a channel that will be closed if any changes happen to CommittedId.
	// Channel can be closed even with no changes to CommittedId.
	CommitNotify() <-chan struct{}
	// Returns EntryId for maximum committed entry.
	CommittedId() int64
	// Returns EntryId for last entry that is stored. May not be committed. TailEntryId can decrease
	// if uncommitted entries get removed due to a writer change.
	TailEntryId() int64
	// Returns range that covers all potentially missing entries: [startId...endId)
	// Returns 0, 0, if there are no missing entries.
	GapRange() (startId int64, endId int64)
	// Returns last committed entry and all the following not-yet committed entries. If `n` is 0, will
	// return all entries, if `n` > 0, will return only up to `n` entries.
	TailEntries(n int) ([]*walleapi.Entry, error)
	// Returns cursor to read committed entries starting at entryId.
	ReadFrom(entryId int64) (Cursor, error)

	// PutEntry puts new entry in storage. PutEntry can handle all types of entries, will return an
	// error if put can't succeed either due to missing data or due to checksum mismatch.
	PutEntry(entry *walleapi.Entry, isCommitted bool) (createdGap bool, err error)
	// CommitEntry marks entry with given EntryId and checksum as committed. Can return an error
	// if entry is missing locally.
	CommitEntry(entryId int64, entryXX uint64) error

	// PutGapEntry is a separate lcall for backfilling missing entries. It is callers responsibility
	// to make sure entries are all valid. For best performance, PutGapEntry calls must be made in
	// monotonically increasing order of EntryId.
	PutGapEntry(entry *walleapi.Entry) error
	UpdateGapStart(entryId int64)
}

// Metadata is expected to be thread-safe.
type Metadata interface {
	StreamURI() string
	// WriterId() string // Perf sensitive. Needs to be in-memory.
	WriterInfo() (writerId walleapi.WriterId, writerAddr string, lease time.Duration, remainingLease time.Duration)
	// Update call is expected to have an internal check to make sure stored writerId never decreases.
	UpdateWriter(writerId walleapi.WriterId, writerAddr string, lease time.Duration) (remainingLease time.Duration, err error)
	RenewLease(writerId walleapi.WriterId, extraBuffer time.Duration) error

	setTopology(topology *walleapi.StreamTopology)
	Topology() *walleapi.StreamTopology
}

// Cursor can be used to read entries from the Stream. It is safe to call Close() on
// already closed cursor. Cursor is also automatically closed once Next() exhausts all
// committed entries.
type Cursor interface {
	// Next moves cursor to the next entry and returns EntryId.
	Next() (entryId int64, ok bool)
	// Entry unmarshals data for EntryId that cursor points to. This call is safe to call only
	// after Next() returns successfully.
	Entry() *walleapi.Entry
	// Closes cursor and its underlying resources.
	Close()
}
