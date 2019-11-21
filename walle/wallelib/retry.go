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
	for retryN := uint(0); ; retryN++ {
		final, err := f(retryN)
		if final || err == nil {
			return err
		}
		backoffTime := (1 << retryN) * minBackoff
		if backoffTime > maxBackoff {
			backoffTime = maxBackoff
		}
		backoffTime = backoffTime/2 + time.Duration(rand.Int63n(int64(backoffTime)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoffTime):
		}
	}
}
