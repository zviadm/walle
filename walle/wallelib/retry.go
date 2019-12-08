package wallelib

import (
	"context"
	"math/rand"
	"time"
)

func KeepTryingWithBackoff(
	ctx context.Context,
	minBackoff time.Duration,
	maxBackoff time.Duration,
	f func(retryN uint) (final bool, err error)) error {
	backoffTime := minBackoff
	for retryN := uint(0); ; retryN++ {
		final, err := f(retryN)
		if final || err == nil {
			return err
		}
		if backoffTime > maxBackoff {
			backoffTime = maxBackoff
		}
		jitteredBackoffTime := backoffTime/2 + time.Duration(rand.Int63n(int64(backoffTime)))
		backoffTime *= 2
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jitteredBackoffTime):
		}
	}
}
