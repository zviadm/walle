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

func (r *request) IsReady(tailId int64) bool {
	if r.Committed && r.Entry != nil {
		return true
	}
	if !r.Committed && r.EntryId <= tailId+1 {
		return true
	}
	return r.EntryId <= tailId
}

// ResultCtx provides Context like interface to wait for a result of queue operation.
type ResultCtx struct {
	mx   sync.Mutex
	done chan struct{}
	err  error
}

func newResult() *ResultCtx {
	return &ResultCtx{done: make(chan struct{})}
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
