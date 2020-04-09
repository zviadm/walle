package storage

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io/ioutil"
	"regexp"
	"strings"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WriterId provides human readable String() method.
type WriterId []byte

// Encode casts WriterId back to []byte type to be used in Protobufs.
func (w WriterId) Encode() []byte {
	return w
}
func (w WriterId) String() string {
	return "w:0x" + hex.EncodeToString(w)
}

const (
	metadataDS = "table:metadata"
	// Global metadata keys
	glbServerId = ".server_id"
	// glbTopology = ".topology"

	// Per streamURI key suffixes
	sfxTopology      = ":topology"
	sfxWriterId      = ":writer_id"
	sfxWriterAddr    = ":writer_addr"
	sfxWriterLeaseNs = ":writer_lease_ns"
	sfxCommittedId   = ":committed_id"
	sfxGapStartId    = ":gap_start_id"
	sfxGapEndId      = ":gap_end_id"

	writerIdLen     = 16
	streamURIMaxLen = 100
)

var (
	// Entry0 is root entry for every WALLE stream.
	Entry0 = &walleapi.Entry{
		EntryId:     0,
		WriterId:    make([]byte, writerIdLen),
		ChecksumMd5: make([]byte, md5.Size),
	}
	entry0B, _    = Entry0.Marshal()
	maxEntryIdKey = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

// MakeWriterId creates new WriterId. WriterId is part random, part based on timestamp,
// so that newer WriterId-s are lexicographically larger.
func MakeWriterId() WriterId {
	writerId := make([]byte, writerIdLen)
	binary.BigEndian.PutUint64(writerId[0:8], uint64(time.Now().UnixNano()))
	rand.Read(writerId[8:writerIdLen])
	return WriterId(writerId)
}

func streamDS(streamURI string) string {
	return "table:stream" + strings.ReplaceAll(streamURI, "/", "-")
}

var reStreamURI = regexp.MustCompile("/[a-z0-9_/]+")

// ValidateStreamURI validates streamURI.
func ValidateStreamURI(streamURI string) error {
	if len(streamURI) > streamURIMaxLen {
		return status.Errorf(codes.InvalidArgument, "streamURI must be at most %d bytes: %s", streamURIMaxLen, streamURI)
	}
	if !reStreamURI.MatchString(streamURI) {
		return status.Errorf(codes.InvalidArgument, "invlaid streamURI: %s", streamURI)
	}
	return nil
}

// TestTmpDir creates new temporary directory that can be used in testing.
func TestTmpDir() string {
	d, err := ioutil.TempDir("", "tt-*")
	panic.OnErr(err)
	return d
}

// IsMember retursn True if given serverId is a member of given stream topology.
func IsMember(t *walleapi.StreamTopology, serverId string) bool {
	for _, sId := range t.ServerIds {
		if serverId == sId {
			return true
		}
	}
	return false
}
