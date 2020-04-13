package pipeline

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultCtx(t *testing.T) {
	r := newResultWithErr(errors.New("error"))
	<-r.Done()
	require.Error(t, r.Err())

	r = newResult()
	select {
	case <-r.Done():
		t.Fatal("must not be done")
	default:
		require.NoError(t, r.Err())
	}
	r.set(errors.New("error"))
	<-r.Done()
	require.Error(t, r.Err())

	r = newResult()
	r.set(nil)
	<-r.Done()
	require.NoError(t, r.Err())
}
