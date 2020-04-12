package wallelib

import "testing"

// BenchmarkCalculateChecksumXX-4 - 7751467 - 146 ns/op - 0 B/op - 0 allocs/op
func BenchmarkCalculateChecksumXX(b *testing.B) {
	var checksum uint64
	data := make([]byte, 1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		checksum = CalculateChecksumXX(checksum, data)
	}
}
