package rpc_size_limits

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/tt/servicelib"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/itest"
	"github.com/zviadm/walle/wallelib"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRpcSizeLimits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer servicelib.KillAll(t)
	_, rootPb, rootCli := itest.SetupRootNodes(t, ctx, 3)

	streamURI := "/t1/size_limits"
	itest.CreateStream(
		t, ctx, rootCli, rootPb.RootUri, streamURI,
		rootPb.Streams[rootPb.RootUri].ServerIds)
	w, err := wallelib.WaitAndClaim(
		ctx, rootCli, streamURI, "blastwriter:1001", time.Second)
	require.NoError(t, err)
	defer w.Close()
	putCtx := w.PutEntry([]byte{})
	<-putCtx.Done()
	require.NoError(t, putCtx.Err())

	// HAX: Manually put entries in a stream, so that they remain uncommitted.
	c, err := rootCli.ForStream("/t1/size_limits")
	require.NoError(t, err)

	entries := []*walleapi.Entry{putCtx.Entry}
	for i := 1; i <= 8; i++ {
		data := make([]byte, wallelib.MaxEntrySize) // Put 8MB worth of data.
		checksum := wallelib.CalculateChecksumMd5(entries[i-1].ChecksumMd5, data)
		entry := &walleapi.Entry{
			EntryId:     entries[i-1].EntryId + 1,
			WriterId:    w.Committed().WriterId,
			Data:        data,
			ChecksumMd5: checksum,
		}
		entries = append(entries, entry)
		_, err := c.PutEntry(ctx, &walleapi.PutEntryRequest{
			StreamUri:         streamURI,
			Entry:             entry,
			CommittedEntryId:  putCtx.Entry.EntryId,
			CommittedEntryMd5: putCtx.Entry.ChecksumMd5,
		})
		require.NoError(t, err)
	}
	tooLarge := make([]byte, wallelib.MaxEntrySize+1)
	checksum := wallelib.CalculateChecksumMd5(entries[len(entries)-1].ChecksumMd5, tooLarge)
	eTooLarge := &walleapi.Entry{
		EntryId:     entries[len(entries)-1].EntryId + 1,
		WriterId:    w.Committed().WriterId,
		Data:        tooLarge,
		ChecksumMd5: checksum,
	}
	_, err = c.PutEntry(ctx, &walleapi.PutEntryRequest{
		StreamUri:         streamURI,
		Entry:             eTooLarge,
		CommittedEntryId:  putCtx.Entry.EntryId,
		CommittedEntryMd5: putCtx.Entry.ChecksumMd5,
	})
	// Make sure MaxEntrySize limits work, and Put gets rejected.
	require.Error(t, err)
	require.EqualValues(t, codes.FailedPrecondition, status.Convert(err).Code())

	zlog.Info("TEST: claiming new writer to resolve large uncommitted entries...")
	w2, err := wallelib.ClaimWriter(
		ctx, rootCli, streamURI, "blastwriter:1001", time.Second)
	require.NoError(t, err)
	require.EqualValues(t, entries[len(entries)-1].EntryId, w2.Committed().EntryId)
}
