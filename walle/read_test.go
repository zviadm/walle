package walle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
	"github.com/zviadm/walle/wallelib"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPollStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, c := newMockSystem(ctx, topo1Node, storage.TestTmpDir())

	streamURI := "/mock/1"
	w, err := wallelib.WaitAndClaim(ctx, c, streamURI, "testhost:1001", time.Second)
	require.NoError(t, err)
	defer w.Close()

	entry, err := c.PollStream(ctx,
		&walleapi.PollStreamRequest{StreamUri: streamURI, PollEntryId: 0})
	require.NoError(t, err)
	require.EqualValues(t, 0, entry.EntryId)

	pCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = c.PollStream(pCtx,
		&walleapi.PollStreamRequest{StreamUri: streamURI, PollEntryId: 1})
	require.Error(t, err)
	require.EqualValues(t, codes.OutOfRange, status.Convert(err).Code())

	entryC := make(chan *walleapi.Entry, 1)
	errC := make(chan error, 1)
	go func() {
		entry, err := c.PollStream(ctx,
			&walleapi.PollStreamRequest{StreamUri: streamURI, PollEntryId: 1})
		entryC <- entry
		errC <- err
	}()
	testData := []byte("hello there")
	putCtx := w.PutEntry(testData)
	<-putCtx.Done()
	require.NoError(t, putCtx.Err())

	err = <-errC
	require.NoError(t, err)
	entry = <-entryC
	require.EqualValues(t, 1, entry.EntryId)
	require.EqualValues(t, testData, entry.Data)
}
