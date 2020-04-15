package storage

import (
	"crypto/rand"
	"encoding/binary"
	"io/ioutil"
	"regexp"
	"strings"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
	"github.com/zviadm/walle/wallelib"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

	streamURIMaxLen = 100
)

var (
	// Entry0 is root entry for every WALLE stream.
	Entry0     = &walleapi.Entry{}
	entry0B, _ = Entry0.Marshal()

	entryMaxSerializedSize = wallelib.MaxEntrySize + 1024 // Add extra 1KB just for safety.
)

// MakeWriterId creates new WriterId. WriterId is part random, part based on timestamp,
// so that newer WriterId-s are lexicographically larger.
func MakeWriterId() walleapi.WriterId {
	id := make([]byte, 8)
	rand.Read(id)
	return walleapi.WriterId{
		Ts: uint64(time.Now().UnixNano()),
		Id: binary.LittleEndian.Uint64(id),
	}
}

// CmpWriterIds compares two writerIds to each other.
func CmpWriterIds(w1, w2 walleapi.WriterId) int {
	if w1.Ts < w2.Ts {
		return -1
	} else if w1.Ts == w2.Ts {
		if w1.Id < w2.Id {
			return -1
		} else if w1.Id == w2.Id {
			return 0
		} else {
			return 1
		}
	} else {
		return 1
	}
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
