package benchmarks

import (
	"testing"

	"github.com/TFMV/arrowgen/zero/decode"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Benchmark Zero-Allocation Mode
func BenchmarkDecodeSimpleZeroAlloc(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		record := createSimpleRecord(pool, size)
		defer record.Release()

		decoder := decode.NewDecoder(record.Schema(), decode.WithAllocator(pool))

		b.Run(formatName("SimpleStruct", size, "ZeroAlloc"), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var users []SimpleUser
				if err := decoder.Decode(record, &users); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
