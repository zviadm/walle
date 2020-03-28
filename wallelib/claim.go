package wallelib

import (
	"context"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"github.com/zviadm/walle/proto/walleapi"
)

// ClaimWriter attempts to forcefully take over as an exclusive writer, even if there
// is another writer already operating healthily. Note that forceful take over can take
// as long as the `writerLease` duration of the current active writer.
func ClaimWriter(
	ctx context.Context,
	c BasicClient,
	streamURI string,
	writerAddr string,
	writerLease time.Duration) (*Writer, *walleapi.Entry, error) {
	cli, err := c.ForStream(streamURI)
	if err != nil {
		return nil, nil, err
	}
	// TODO(zviad): if previous writer has much larger lease, this will timeout.
	ctx, cancel := context.WithTimeout(ctx, writerLease*3)
	defer cancel()
	resp, err := cli.ClaimWriter(
		ctx, &walleapi.ClaimWriterRequest{
			StreamUri:  streamURI,
			WriterAddr: writerAddr,
			LeaseMs:    writerLease.Nanoseconds() / time.Millisecond.Nanoseconds(),
		})
	if err != nil {
		return nil, nil, errors.Wrap(err, "")
	}
	commitTime := time.Now()
	_, err = cli.PutEntry(ctx, &walleapi.PutEntryRequest{
		StreamUri:         streamURI,
		Entry:             &walleapi.Entry{WriterId: resp.WriterId},
		CommittedEntryId:  resp.LastEntry.EntryId,
		CommittedEntryMd5: resp.LastEntry.ChecksumMd5,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "")
	}
	w := newWriter(
		c, streamURI,
		writerLease, writerAddr,
		resp.WriterId, resp.LastEntry, commitTime)
	return w, resp.LastEntry, nil
}

// WaitAndClaim only attempts to claim, once current writer is no longer actively heartbeating.
// WaitAndClaim will continue to wait and attempt claims until it is successful or `ctx` expires.
func WaitAndClaim(
	ctx context.Context,
	c BasicClient,
	streamURI string,
	writerAddr string,
	writerLease time.Duration) (w *Writer, e *walleapi.Entry, err error) {
	err = KeepTryingWithBackoff(ctx, writerLease/2, writerLease/2,
		func(retryN uint) (bool, error) {
			s, err := c.ForStream(streamURI)
			if err != nil {
				return false, err
			}
			var status *walleapi.WriterStatusResponse
			for {
				statusCtx, cancel := context.WithTimeout(ctx, writerLease)
				status, err = s.WriterStatus(
					statusCtx, &walleapi.WriterStatusRequest{StreamUri: streamURI})
				cancel()
				if err != nil {
					return false, err
				}
				if status.RemainingLeaseMs <= 0 {
					break
				}
				sleepTime := time.Duration(status.RemainingLeaseMs)*time.Millisecond +
					time.Duration(rand.Int63n(int64(writerLease/4)))
				select {
				case <-ctx.Done():
					return true, ctx.Err()
				case <-time.After(sleepTime):
				}
			}
			w, e, err = ClaimWriter(ctx, c, streamURI, writerAddr, writerLease)
			return err == nil, err
		})
	if err != nil {
		return nil, nil, err
	}
	return
}
