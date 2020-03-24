package wallelib

import (
	"context"
	"time"

	"github.com/golang/glog"
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
	retryDelay := writerLease / 10
	if time.Second > retryDelay {
		retryDelay = time.Second
	}
	err = KeepTryingWithBackoff(ctx, retryDelay, retryDelay,
		func(retryN uint) (bool, error) {
			s, err := c.ForStream(streamURI)
			if err != nil {
				glog.Warningf("[%s] writer: %s err: %s...", streamURI, writerAddr, err)
				return false, err
			}
			var status *walleapi.WriterStatusResponse
			for {
				status, err = s.WriterStatus(ctx, &walleapi.WriterStatusRequest{StreamUri: streamURI})
				if err != nil {
					glog.Warningf("[%s] writer: %s err: %s...", streamURI, writerAddr, err)
					return false, err
				}
				if status.RemainingLeaseMs == 0 {
					break
				}
				select {
				case <-ctx.Done():
					return true, ctx.Err()
				case <-time.After(time.Duration(status.RemainingLeaseMs) * time.Millisecond):
				}
			}
			w, e, err = ClaimWriter(ctx, c, streamURI, writerAddr, writerLease)
			if err != nil {
				glog.Warningf(
					"[%s] writer: %s claim attempt (%s: %d): %s...",
					streamURI, writerAddr, status.WriterAddr, status.RemainingLeaseMs, err)
			}
			return err == nil, err
		})
	if err != nil {
		return nil, nil, err
	}
	return
}
