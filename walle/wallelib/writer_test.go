package wallelib

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriterSimple(t *testing.T) {
	m := newMockSystem([]string{"1", "2", "3"})
	w := newWriter(m, "w1", m.servers["1"].entries[0])
	defer w.Close()
	e1, c1 := w.PutEntry([]byte("d1"))
	e2, c2 := w.PutEntry([]byte("d2"))
	require.EqualValues(t, e1.EntryId, 1, "e1: %+v", e1)
	require.EqualValues(t, e2.EntryId, 2, "e2: %+v", e2)

	err := <-c2
	require.NoError(t, err)
	select {
	case err := <-c1:
		require.NoError(t, err)
	default:
		t.Fatalf("c1 must have been ready since c2 was ready")
	}

	e3, c3 := w.PutEntry([]byte("d3"))
	require.EqualValues(t, e3.EntryId, 3, "e3: %+v", e3)
	err = <-c3
	require.NoError(t, err)

	require.EqualValues(t, m.servers["1"].committed, 2)
	require.EqualValues(t, m.servers["2"].committed, 2)
	require.EqualValues(t, m.servers["3"].committed, 2)
}
