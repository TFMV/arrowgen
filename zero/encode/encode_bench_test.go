package encode

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Test structs for benchmarking
type SimpleUser struct {
	ID       int64  `arrow:"id"`
	Name     string `arrow:"name"`
	Email    string `arrow:"email"`
	IsActive bool   `arrow:"is_active"`
}

type AllTypes struct {
	Int8      int8      `arrow:"int8"`
	Int16     int16     `arrow:"int16"`
	Int32     int32     `arrow:"int32"`
	Int64     int64     `arrow:"int64"`
	Uint8     uint8     `arrow:"uint8"`
	Uint16    uint16    `arrow:"uint16"`
	Uint32    uint32    `arrow:"uint32"`
	Uint64    uint64    `arrow:"uint64"`
	Float32   float32   `arrow:"float32"`
	Float64   float64   `arrow:"float64"`
	String    string    `arrow:"string"`
	Bool      bool      `arrow:"bool"`
	Timestamp time.Time `arrow:"timestamp"`
	Binary    []byte    `arrow:"binary"`
}

// generateSimpleUsers creates test data for SimpleUser benchmarks
func generateSimpleUsers(n int) []SimpleUser {
	users := make([]SimpleUser, n)
	for i := 0; i < n; i++ {
		users[i] = SimpleUser{
			ID:       int64(i),
			Name:     "User Name",
			Email:    "user@example.com",
			IsActive: true,
		}
	}
	return users
}

// generateAllTypes creates test data for AllTypes benchmarks
func generateAllTypes(n int) []AllTypes {
	data := make([]AllTypes, n)
	now := time.Now()
	for i := 0; i < n; i++ {
		data[i] = AllTypes{
			Int8:      42,
			Int16:     4242,
			Int32:     424242,
			Int64:     42424242,
			Uint8:     42,
			Uint16:    4242,
			Uint32:    424242,
			Uint64:    42424242,
			Float32:   42.42,
			Float64:   4242.4242,
			String:    "test string",
			Bool:      true,
			Timestamp: now,
			Binary:    []byte("test binary data"),
		}
	}
	return data
}

// generateMapData creates test data for map benchmarks
func generateMapData(n int) []map[string]interface{} {
	data := make([]map[string]interface{}, n)
	now := time.Now()
	for i := 0; i < n; i++ {
		data[i] = map[string]interface{}{
			"id":        int64(i),
			"name":      "User Name",
			"email":     "user@example.com",
			"timestamp": now,
			"active":    true,
		}
	}
	return data
}

// createSimpleSchema creates a schema for SimpleUser
func createSimpleSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)
}

// createAllTypesSchema creates a schema for AllTypes
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

// createMapSchema creates a schema for map data
func createMapSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)
}

func BenchmarkEncodeSimpleZeroAlloc(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		data := generateSimpleUsers(size)
		schema := createSimpleSchema()
		enc := NewEncoder(schema, WithMode(ModeZeroAlloc), WithAllocator(pool))

		b.Run(formatBenchName("SimpleStruct", size, "ZeroAlloc"), func(b *testing.B) {
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

func BenchmarkEncodeSimpleHighThroughput(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		data := generateSimpleUsers(size)
		schema := createSimpleSchema()
		enc := NewEncoder(schema, WithMode(ModeHighThroughput), WithAllocator(pool))

		b.Run(formatBenchName("SimpleStruct", size, "HighThroughput"), func(b *testing.B) {
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

func BenchmarkEncodeAllTypesZeroAlloc(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		data := generateAllTypes(size)
		schema := createAllTypesSchema()
		enc := NewEncoder(schema, WithMode(ModeZeroAlloc), WithAllocator(pool))

		b.Run(formatBenchName("AllTypes", size, "ZeroAlloc"), func(b *testing.B) {
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

func BenchmarkEncodeAllTypesHighThroughput(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		data := generateAllTypes(size)
		schema := createAllTypesSchema()
		enc := NewEncoder(schema, WithMode(ModeHighThroughput), WithAllocator(pool))

		b.Run(formatBenchName("AllTypes", size, "HighThroughput"), func(b *testing.B) {
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

func BenchmarkEncodeMapZeroAlloc(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		data := generateMapData(size)
		schema := createMapSchema()
		enc := NewEncoder(schema, WithMode(ModeZeroAlloc), WithAllocator(pool))

		b.Run(formatBenchName("Map", size, "ZeroAlloc"), func(b *testing.B) {
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

func BenchmarkEncodeMapHighThroughput(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		data := generateMapData(size)
		schema := createMapSchema()
		enc := NewEncoder(schema, WithMode(ModeHighThroughput), WithAllocator(pool))

		b.Run(formatBenchName("Map", size, "HighThroughput"), func(b *testing.B) {
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

// formatBenchName formats the benchmark name with size and mode
func formatBenchName(typ string, size int, mode string) string {
	return typ + "/" + mode + "/" + formatSize(size)
}

// formatSize formats the size with appropriate units
func formatSize(size int) string {
	switch {
	case size >= 1000000:
		return string(rune(size/1000000)) + "M"
	case size >= 1000:
		return string(rune(size/1000)) + "K"
	default:
		return string(rune(size))
	}
}
