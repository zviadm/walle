package wallelib

import (
	"context"
	"math/rand"
	"time"

	"github.com/zviadm/walle/proto/walleapi"
)

// ClaimWriter attempts to forcefully take over as an exclusive writer, even if there
// is another writer already operating healthily. Note that forceful take over can take
// as long as the `writerLease` duration of the current active writer.
func ClaimWriter(
	ctx context.Context,
	c Client,
	streamURI string,
	writerAddr string,
	writerLease time.Duration) (*Writer, error) {
	cli, err := c.ForStream(streamURI)
	if err != nil {
		return nil, err
	}
	return claimWriter(ctx, c, cli, streamURI, writerAddr, writerLease)
}

func claimWriter(
	ctx context.Context,
	c Client,
	cli walleapi.WalleApiClient,
	streamURI string,
	writerAddr string,
	writerLease time.Duration) (*Writer, error) {
	// TODO(zviad): if previous writer has much larger lease
	// and is still active, this can timeout.
	ctx, cancel := context.WithTimeout(ctx, writerLease*3)
	defer cancel()
	resp, err := cli.ClaimWriter(
		ctx, &walleapi.ClaimWriterRequest{
			StreamUri:  streamURI,
			WriterAddr: writerAddr,
			LeaseMs:    writerLease.Nanoseconds() / time.Millisecond.Nanoseconds(),
		})
	if err != nil {
		return nil, err
	}
	// Make first hearbeat call synchronously to refresh lease timer before returning
	// the writer object.
	commitTime := time.Now()
	_, err = cli.PutEntry(ctx, &walleapi.PutEntryRequest{
		StreamUri:        streamURI,
		Entry:            &walleapi.Entry{WriterId: resp.WriterId},
		CommittedEntryId: resp.TailEntry.EntryId,
		CommittedEntryXX: resp.TailEntry.ChecksumXX,
	})
	if err != nil {
		return nil, err
	}
	w := newWriter(
		c, streamURI,
		writerLease, writerAddr,
		resp.WriterId, resp.TailEntry, commitTime)
	return w, nil
}

// WaitAndClaim only attempts to claim once current writer is no longer actively heartbeating.
// WaitAndClaim will continue to wait and attempt claims until it is successful or `ctx` expires.
func WaitAndClaim(
	ctx context.Context,
	c Client,
	streamURI string,
	writerAddr string,
	writerLease time.Duration) (w *Writer, err error) {
	err = KeepTryingWithBackoff(ctx, writerLease/2, writerLease/2,
		func(retryN uint) (bool, bool, error) {
			s, err := c.ForStream(streamURI)
			if err != nil {
				return false, false, err
			}
			var status *walleapi.WriterStatusResponse
			for {
				statusCtx, cancel := context.WithTimeout(ctx, writerLease/4)
				status, err = s.WriterStatus(
					statusCtx, &walleapi.WriterStatusRequest{StreamUri: streamURI})
				cancel()
				if err != nil {
					return false, false, err
				}
				if status.RemainingLeaseMs <= 0 {
					break
				}
				sleepTime := time.Duration(status.RemainingLeaseMs)*time.Millisecond +
					time.Duration(rand.Int63n(int64(writerLease/4)))
				select {
				case <-ctx.Done():
					return true, false, ctx.Err()
				case <-time.After(sleepTime):
				}
			}
			// Use same ForStream client that already returned successful result for `WriterStatus` call.
			// This helps to avoid any unneccessary timeouts if some other node is in a questionable state.
			w, err = claimWriter(ctx, c, s, streamURI, writerAddr, writerLease)
			return err == nil, false, err
		})
	if err != nil {
		return nil, err
	}
	return
}
