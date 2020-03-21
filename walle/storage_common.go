package walle

import (
	"crypto/md5"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
)

const (
	metadataDS = "table:metadata"
	// Global metadata keys
	glbServerId = ".server_id"
	glbTopology = ".topology"

	// Per streamURI key suffixes
	sfxTopology         = ":topology"
	sfxWriterId         = ":writer_id"
	sfxCommittedId      = ":committed_id"
	sfxNoGapCommittedId = ":no_gap_comitted_id"

	writerIdLen = 16
	serverIdLen = 16
)

var (
	maxEntryIdKey = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	entry0        = &walleapi.Entry{EntryId: 0, ChecksumMd5: make([]byte, md5.Size)}
	entry0B, _    = entry0.Marshal()
)

func streamDS(streamURI string) string {
	return "table:stream" + strings.ReplaceAll(streamURI, "/", "-")
}

var reStreamURI = regexp.MustCompile("/[a-z0-9_/]+")

func isValidStreamURI(streamURI string) error {
	if len(streamURI) > 100 {
		return errors.Errorf("streamURI must be at most 100 bytes: %s", streamURI)

	}
	if !reStreamURI.MatchString(streamURI) {
		return errors.Errorf("invlaid streamURI: %s", streamURI)
	}
	return nil
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
func panicOnNotOk(ok bool, msg string) {
	if !ok {
		panic(msg)
	}
}

func StorageTmpTestDir() string {
	d, err := ioutil.TempDir("", "tt-*")
	panicOnErr(err)
	return d
}
