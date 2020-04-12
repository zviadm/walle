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
	defer servicelib.KillAll()
	_, rootPb, rootCli := itest.SetupRootNodes(ctx, t, 3)

	streamURI := "/t1/size_limits"
	itest.CreateStream(
		ctx, t, rootCli, rootPb.RootUri, streamURI,
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
		checksum := wallelib.CalculateChecksumXX(entries[i-1].ChecksumXX, data)
		entry := &walleapi.Entry{
			EntryId:    entries[i-1].EntryId + 1,
			WriterId:   w.Committed().WriterId,
			Data:       data,
			ChecksumXX: checksum,
		}
		entries = append(entries, entry)
		_, err := c.PutEntry(ctx, &walleapi.PutEntryRequest{
			StreamUri:        streamURI,
			Entry:            entry,
			CommittedEntryId: putCtx.Entry.EntryId,
			CommittedEntryXX: putCtx.Entry.ChecksumXX,
		})
		require.NoError(t, err)
	}
	tooLarge := make([]byte, wallelib.MaxEntrySize+1)
	checksum := wallelib.CalculateChecksumXX(entries[len(entries)-1].ChecksumXX, tooLarge)
	eTooLarge := &walleapi.Entry{
		EntryId:    entries[len(entries)-1].EntryId + 1,
		WriterId:   w.Committed().WriterId,
		Data:       tooLarge,
		ChecksumXX: checksum,
	}
	_, err = c.PutEntry(ctx, &walleapi.PutEntryRequest{
		StreamUri:        streamURI,
		Entry:            eTooLarge,
		CommittedEntryId: putCtx.Entry.EntryId,
		CommittedEntryXX: putCtx.Entry.ChecksumXX,
	})
	// Make sure MaxEntrySize limits work, and Put gets rejected.
	require.Error(t, err)
	require.EqualValues(t, codes.InvalidArgument, status.Convert(err).Code())

	zlog.Info("TEST: claiming new writer to resolve large uncommitted entries...")
	w2, err := wallelib.ClaimWriter(
		ctx, rootCli, streamURI, "blastwriter:1001", time.Second)
	require.NoError(t, err)
	require.EqualValues(t, entries[len(entries)-1].EntryId, w2.Committed().EntryId)
}
