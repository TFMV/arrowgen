package benchmarks

import (
	"strconv"
	"testing"
	"time"

	"github.com/TFMV/arrowgen/zero/encode"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Generate test data
func generateSimpleUsers(n int) []SimpleUser {
	users := make([]SimpleUser, n)
	for i := 0; i < n; i++ {
		users[i] = SimpleUser{
			ID:    int64(i),
			Name:  "User Name",
			Email: "user@example.com",
			Age:   30,
		}
	}
	return users
}

func generateAllTypes(n int) []AllTypes {
	data := make([]AllTypes, n)
	now := time.Now()
	for i := 0; i < n; i++ {
		data[i] = AllTypes{
			Int8:      8,
			Int16:     16,
			Int32:     32,
			Int64:     64,
			Uint8:     8,
			Uint16:    16,
			Uint32:    32,
			Uint64:    64,
			Float32:   32.0,
			Float64:   64.0,
			String:    "test" + strconv.Itoa(i),
			Bool:      i%2 == 0,
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Binary:    []byte("test" + strconv.Itoa(i)),
		}
	}
	return data
}

// Benchmark encoding all types
func BenchmarkEncodeAllTypes(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		data := generateAllTypes(size)
		schema := createAllTypesSchema()

		// Test Zero-Allocation mode
		b.Run(formatName("AllTypes", size, "ZeroAlloc"), func(b *testing.B) {
			enc := encode.NewEncoder(schema, encode.WithMode(encode.ModeZeroAlloc), encode.WithAllocator(pool))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				record, err := enc.Encode(data)
				if err != nil {
					b.Fatal(err)
				}
				record.Release()
			}
		})

		// Test High-Throughput mode
		b.Run(formatName("AllTypes", size, "HighThroughput"), func(b *testing.B) {
			enc := encode.NewEncoder(schema, encode.WithMode(encode.ModeHighThroughput), encode.WithAllocator(pool))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				record, err := enc.Encode(data)
				if err != nil {
					b.Fatal(err)
				}
				record.Release()
			}
		})
	}
}

// Create schemas for benchmarking
func createSimpleSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
}

func createAllTypesSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "int8", Type: arrow.PrimitiveTypes.Int8},
		{Name: "int16", Type: arrow.PrimitiveTypes.Int16},
		{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "uint8", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "uint16", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "uint32", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "uint64", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "float32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "float64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "string", Type: arrow.BinaryTypes.String},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: "binary", Type: arrow.BinaryTypes.Binary},
	}, nil)
}

// Benchmark Zero-Allocation Mode
func BenchmarkEncodeSimpleZeroAlloc(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		data := generateSimpleUsers(size)
		schema := createSimpleSchema()
		enc := encode.NewEncoder(schema, encode.WithMode(encode.ModeZeroAlloc), encode.WithAllocator(pool))

		b.Run(formatName("SimpleStruct", size, "ZeroAlloc"), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				record, err := enc.Encode(data)
				if err != nil {
					b.Fatal(err)
				}
				record.Release()
			}
		})
	}
}

// Benchmark High-Throughput Mode
func BenchmarkEncodeSimpleHighThroughput(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		data := generateSimpleUsers(size)
		schema := createSimpleSchema()
		enc := encode.NewEncoder(schema, encode.WithMode(encode.ModeHighThroughput), encode.WithAllocator(pool))

		b.Run(formatName("SimpleStruct", size, "HighThroughput"), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				record, err := enc.Encode(data)
				if err != nil {
					b.Fatal(err)
				}
				record.Release()
			}
		})
	}
}

// Helper function to format benchmark names
func formatName(structure string, size int, mode string) string {
	return structure + "/" + strconv.Itoa(size) + "/" + mode
}
