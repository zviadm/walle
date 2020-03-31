package pipeline

import (
	"sync"

	"github.com/zviadm/walle/proto/walleapi"
)

type Request struct {
	EntryId   int64
	EntryMd5  []byte
	Committed bool
	Entry     *walleapi.Entry
}

func (r *Request) IsReady(tailId int64) bool {
	if r.Committed && r.Entry != nil {
		return true
	}
	if !r.Committed && r.EntryId <= tailId+1 {
		return true
	}
	return r.EntryId <= tailId
}

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
func (r *ResultCtx) Done() <-chan struct{} {
	return r.done
}
func (r *ResultCtx) Err() error {
	r.mx.Lock()
	defer r.mx.Unlock()
	return r.err
}
