package benchmarks

import (
	"testing"
	"time"

	"github.com/TFMV/arrowgen/zero/decode"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Test struct with all supported types
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

// Simple struct for basic benchmarks
type SimpleUser struct {
	ID    int64  `arrow:"id"`
	Name  string `arrow:"name"`
	Email string `arrow:"email"`
	Age   int32  `arrow:"age"`
}

func createSimpleRecord(pool memory.Allocator, numRows int) arrow.Record {
	// Create builders
	idBuilder := array.NewInt64Builder(pool)
	nameBuilder := array.NewStringBuilder(pool)
	emailBuilder := array.NewStringBuilder(pool)
	ageBuilder := array.NewInt32Builder(pool)
	defer idBuilder.Release()
	defer nameBuilder.Release()
	defer emailBuilder.Release()
	defer ageBuilder.Release()

	// Add data
	for i := 0; i < numRows; i++ {
		idBuilder.Append(int64(i))
		nameBuilder.Append("User " + string(rune(i%26+65)))
		emailBuilder.Append("user" + string(rune(i%26+65)) + "@example.com")
		ageBuilder.Append(int32(20 + (i % 50)))
	}

	// Create arrays
	cols := []arrow.Array{
		idBuilder.NewArray(),
		nameBuilder.NewArray(),
		emailBuilder.NewArray(),
		ageBuilder.NewArray(),
	}
	defer func() {
		for _, col := range cols {
			col.Release()
		}
	}()

	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "email", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	// Create record
	return array.NewRecord(schema, cols, int64(numRows))
}

func createAllTypesRecord(pool memory.Allocator, numRows int) arrow.Record {
	// Create builders for all types
	int8Builder := array.NewInt8Builder(pool)
	int16Builder := array.NewInt16Builder(pool)
	int32Builder := array.NewInt32Builder(pool)
	int64Builder := array.NewInt64Builder(pool)
	uint8Builder := array.NewUint8Builder(pool)
	uint16Builder := array.NewUint16Builder(pool)
	uint32Builder := array.NewUint32Builder(pool)
	uint64Builder := array.NewUint64Builder(pool)
	float32Builder := array.NewFloat32Builder(pool)
	float64Builder := array.NewFloat64Builder(pool)
	stringBuilder := array.NewStringBuilder(pool)
	boolBuilder := array.NewBooleanBuilder(pool)
	timestampBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Nanosecond})
	binaryBuilder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)

	defer func() {
		int8Builder.Release()
		int16Builder.Release()
		int32Builder.Release()
		int64Builder.Release()
		uint8Builder.Release()
		uint16Builder.Release()
		uint32Builder.Release()
		uint64Builder.Release()
		float32Builder.Release()
		float64Builder.Release()
		stringBuilder.Release()
		boolBuilder.Release()
		timestampBuilder.Release()
		binaryBuilder.Release()
	}()

	// Add data
	now := time.Now()
	for i := 0; i < numRows; i++ {
		int8Builder.Append(int8(i % 128))
		int16Builder.Append(int16(i))
		int32Builder.Append(int32(i))
		int64Builder.Append(int64(i))
		uint8Builder.Append(uint8(i % 128))
		uint16Builder.Append(uint16(i))
		uint32Builder.Append(uint32(i))
		uint64Builder.Append(uint64(i))
		float32Builder.Append(float32(i) * 1.5)
		float64Builder.Append(float64(i) * 1.5)
		stringBuilder.Append("value" + string(rune(i%26+65)))
		boolBuilder.Append(i%2 == 0)
		timestampBuilder.Append(arrow.Timestamp(now.Add(time.Duration(i) * time.Hour).UnixNano()))
		binaryBuilder.Append([]byte("binary" + string(rune(i%26+65))))
	}

	// Create arrays
	cols := []arrow.Array{
		int8Builder.NewArray(),
		int16Builder.NewArray(),
		int32Builder.NewArray(),
		int64Builder.NewArray(),
		uint8Builder.NewArray(),
		uint16Builder.NewArray(),
		uint32Builder.NewArray(),
		uint64Builder.NewArray(),
		float32Builder.NewArray(),
		float64Builder.NewArray(),
		stringBuilder.NewArray(),
		boolBuilder.NewArray(),
		timestampBuilder.NewArray(),
		binaryBuilder.NewArray(),
	}
	defer func() {
		for _, col := range cols {
			col.Release()
		}
	}()

	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
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
			{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}},
			{Name: "binary", Type: arrow.BinaryTypes.Binary},
		},
		nil,
	)

	// Create record
	return array.NewRecord(schema, cols, int64(numRows))
}

// Benchmark decoding simple structs with different row counts
func BenchmarkDecodeSimple(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		record := createSimpleRecord(pool, size)
		defer record.Release()

		decoder := decode.NewDecoder(record.Schema())
		var users []SimpleUser

		b.Run("SimpleStruct/"+string(rune(size)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				users = nil
				if err := decoder.Decode(record, &users); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark decoding all types with different row counts
func BenchmarkDecodeAllTypes(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		record := createAllTypesRecord(pool, size)
		defer record.Release()

		decoder := decode.NewDecoder(record.Schema())
		var data []AllTypes

		b.Run("AllTypes/"+string(rune(size)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data = nil
				if err := decoder.Decode(record, &data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark decoding into maps
func BenchmarkDecodeMap(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	pool := memory.NewGoAllocator()

	for _, size := range sizes {
		record := createSimpleRecord(pool, size)
		defer record.Release()

		decoder := decode.NewDecoder(record.Schema())
		var maps []map[string]interface{}

		b.Run("Map/"+string(rune(size)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				maps = nil
				if err := decoder.Decode(record, &maps); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark with custom memory allocator
func BenchmarkDecodeWithAllocator(b *testing.B) {
	size := 10000
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	record := createSimpleRecord(pool, size)
	defer record.Release()

	decoder := decode.NewDecoder(record.Schema(), decode.WithAllocator(pool))
	var users []SimpleUser

	b.Run("CustomAllocator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			users = nil
			if err := decoder.Decode(record, &users); err != nil {
				b.Fatal(err)
			}
		}
	})
}
