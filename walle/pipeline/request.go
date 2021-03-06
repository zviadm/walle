package pipeline

import (
	"sync"

	"github.com/zviadm/walle/proto/walleapi"
)

type request struct {
	EntryId   int64
	EntryXX   uint64
	Committed bool
	Entry     *walleapi.Entry
}

// ResultCtx provides Context like interface to wait for a result of queue operation.
type ResultCtx struct {
	mx   sync.Mutex
	done chan struct{}
	err  error
}

var (
	closedDone = func() chan struct{} {
		r := make(chan struct{})
		close(r)
		return r
	}()
)

func newResult() *ResultCtx {
	return &ResultCtx{done: make(chan struct{})}
}
func newResultWithErr(err error) *ResultCtx {
	return &ResultCtx{done: closedDone, err: err}
}
func (r *ResultCtx) set(err error) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.err = err
	close(r.done)
}

// Done returns channel that will be closed when result is produced.
func (r *ResultCtx) Done() <-chan struct{} {
	return r.done
}

// Err returns result of the operation. This call is only relevant once
// channel returned by Done() is closed.
func (r *ResultCtx) Err() error {
	r.mx.Lock()
	defer r.mx.Unlock()
	return r.err
}
