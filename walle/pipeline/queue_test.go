package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
)

func TestPipelineQueue(t *testing.T) {
	q := newQueue()
	res1 := q.Queue(&Request{EntryId: 1, Committed: true})
	res2 := q.Queue(&Request{EntryId: 1, Entry: &walleapi.Entry{EntryId: 1}})
	require.Equal(t, res1, res2)
	// TODO(zviad): More tests.

	// _ = newPipelineQueue()
	// q.Push(&walle_pb.PutEntryInternalRequest{})

}
