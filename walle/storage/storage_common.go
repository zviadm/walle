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

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/panic"
)

type WriterId string

func (w WriterId) Encode() string {
	return string(w)
}
func (w WriterId) String() string {
	return hex.EncodeToString([]byte(w))
}

// func (s ServerId) Encode() string {
// 	return string(s)
// }
// func (s ServerId) String() string {
// 	return hex.EncodeToString([]byte(s))
// }

const (
	metadataDS = "table:metadata"
	// Global metadata keys
	glbServerId = ".server_id"
	glbTopology = ".topology"

	// Per streamURI key suffixes
	sfxTopology      = ":topology"
	sfxWriterId      = ":writer_id"
	sfxWriterAddr    = ":writer_addr"
	sfxWriterLeaseNs = ":writer_lease_ns"
	sfxCommittedId   = ":committed_id"
	sfxGapStartId    = ":gap_start_id"
	sfxGapEndId      = ":gap_end_id"

	writerIdLen = 16
	serverIdLen = 16

	streamURIMaxLen = 100
)

var (
	Entry0 = &walleapi.Entry{
		EntryId:     0,
		WriterId:    string(make([]byte, writerIdLen)),
		ChecksumMd5: make([]byte, md5.Size),
	}
	entry0B, _    = Entry0.Marshal()
	maxEntryIdKey = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

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

func IsValidStreamURI(streamURI string) error {
	if len(streamURI) > streamURIMaxLen {
		return errors.Errorf("streamURI must be at most %d bytes: %s", streamURIMaxLen, streamURI)
	}
	if !reStreamURI.MatchString(streamURI) {
		return errors.Errorf("invlaid streamURI: %s", streamURI)
	}
	return nil
}

func TestTmpDir() string {
	d, err := ioutil.TempDir("", "tt-*")
	panic.OnErr(err)
	return d
}
