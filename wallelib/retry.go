package wallelib

import (
	"context"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"github.com/zviadm/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KeepTryingWithBackoff is a helper function to continue retrying given
// function with an exponential backoff. Function is retried until it is successful,
// or returns final=True, or context expires.
func KeepTryingWithBackoff(
	ctx context.Context,
	minBackoff time.Duration,
	maxBackoff time.Duration,
	f func(retryN uint) (final bool, silent bool, err error)) error {
	backoffTime := minBackoff
	for retryN := uint(0); ; retryN++ {
		callTs := time.Now()
		final, silent, err := f(retryN)
		if final || err == nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		isDeadlineErr := (err == context.DeadlineExceeded || status.Convert(err).Code() == codes.DeadlineExceeded)
		if (retryN >= 2 || isDeadlineErr) && !silent {
			stackTrace := errors.Wrap(err, "").(stackTracer).StackTrace()
			stackFrame := stackTrace[1]
			zlog.Warningf("%n (%s:%d) retry #%d: %s...",
				stackFrame, stackFrame, stackFrame, retryN, err)
		}
		if backoffTime > maxBackoff {
			backoffTime = maxBackoff
		}
		jitteredBackoffTime := backoffTime/2 + time.Duration(rand.Int63n(int64(backoffTime)))
		backoffTime *= 2
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(callTs.Add(jitteredBackoffTime).Sub(time.Now())):
		}
	}
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}
