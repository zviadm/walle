package broadcast

import (
	"context"
	"time"

	"github.com/pkg/errors"
	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/wallelib"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Client defines interface for a direct accessing client that can talk to specific
// serverId-s.
type Client interface {
	ForServer(serverId string) (walle_pb.WalleClient, error)
}

// Call broadcasts requests to all serverIds and returns list of serverIds that have succeeded.
// Returns an error if majority didn't succeed.
func Call(
	ctx context.Context,
	cli Client,
	serverIds []string,
	waitLive time.Duration,
	waitBG time.Duration,
	call func(
		c walle_pb.WalleClient,
		ctx context.Context,
		serverId string) error) (successIds []string, err error) {
	callCtx, cancelCalls := context.WithCancel(context.Background())
	callStart := time.Now()
	defer func() {
		if err != nil || callCtx.Err() != nil {
			cancelCalls()
			return
		}
		sleepTimeLive := time.Now().Add(waitLive).Sub(callStart)
		if sleepTimeLive > 0 {
			select {
			case <-callCtx.Done():
				return
			case <-time.After(sleepTimeLive):
			}
		}
		sleepTimeBG := time.Now().Add(waitBG).Sub(callStart)
		if sleepTimeBG <= 0 || callCtx.Err() != nil {
			return
		}
		go func() {
			select {
			case <-callCtx.Done():
			case <-time.After(sleepTimeBG):
				cancelCalls()
			}
		}()
	}()
	type callErr struct {
		ServerId string
		Err      error
	}
	errsC := make(chan *callErr, len(serverIds))
	callsInFlight := atomic.NewInt64(int64(len(serverIds)))
	for _, serverId := range serverIds {
		c, err := cli.ForServer(serverId)
		if err != nil {
			errsC <- &callErr{ServerId: serverId, Err: err}
			if callsInFlight.Add(-1) == 0 {
				cancelCalls()
			}
			continue
		}
		go func(c walle_pb.WalleClient, serverId string) {
			err := call(c, callCtx, serverId)
			errsC <- &callErr{ServerId: serverId, Err: err}
			if callsInFlight.Add(-1) == 0 {
				cancelCalls()
			}
		}(c, serverId)
	}
	var errCodeFinal codes.Code
	var errs []error
	for i := 0; i < len(serverIds); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-errsC:
			if err.Err != nil {
				errs = append(errs, errors.Wrap(err.Err, err.ServerId))
				errCode := status.Convert(err.Err).Code()
				if wallelib.IsErrFinal(errCode) || !wallelib.IsErrFinal(errCodeFinal) {
					errCodeFinal = errCode
				}
			} else {
				successIds = append(successIds, err.ServerId)
			}
		}
		if len(successIds) >= len(serverIds)/2+1 {
			return successIds, nil
		}
		if len(errs) >= (len(serverIds)+1)/2 {
			return nil, status.Errorf(errCodeFinal, "errs: %d / %d - %s", len(errs), len(serverIds), errs)
		}
	}
	panic("unreachable code")
}
