package schema

import (
	"testing"
	"time"
)

// Benchmark data structures
type BenchSimpleStruct struct {
	Int    int     `arrow:"int"`
	String string  `arrow:"string"`
	Float  float64 `arrow:"float"`
	Bool   bool    `arrow:"bool"`
}

type BenchComplexStruct struct {
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
	Time      time.Time `arrow:"timestamp"`
	ByteSlice []byte    `arrow:"bytes"`
}

type BenchNestedStruct struct {
	ID     int64             `arrow:"id"`
	Nested BenchSimpleStruct `arrow:"nested"`
	Array  []int32           `arrow:"array"`
}

// Simple map benchmarks
func generateMapData(n int) map[string]interface{} {
	m := make(map[string]interface{}, n)
	for i := 0; i < n; i++ {
		key := "key" + string(rune(i+65)) // A, B, C, ...
		if i%5 == 0 {
			m[key] = i
		} else if i%5 == 1 {
			m[key] = float64(i) * 1.5
		} else if i%5 == 2 {
			m[key] = "string" + string(rune(i+97)) // a, b, c, ...
		} else if i%5 == 3 {
			m[key] = i%2 == 0
		} else {
			m[key] = time.Now().Add(time.Duration(i) * time.Hour)
		}
	}
	return m
}

// Nested map benchmarks
func generateNestedMapData(n int) map[string]interface{} {
	m := make(map[string]interface{})

	// Add some basic types
	m["int"] = int64(42)
	m["float"] = float64(3.14)
	m["string"] = "nested"
	m["bool"] = true

	// Add a nested map with concrete values instead of potentially nil interfaces
	nestedMap := make(map[string]int64)
	for i := 0; i < n; i++ {
		key := "nested" + string(rune(i+65)) // A, B, C, ...
		nestedMap[key] = int64(i)
	}
	m["map"] = nestedMap

	return m
}

// Benchmarks for inferring schema from structs
func BenchmarkSchemaFromSimpleStruct(b *testing.B) {
	s := BenchSimpleStruct{
		Int:    42,
		String: "test",
		Float:  3.14,
		Bool:   true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := SchemaFromStruct(s)
		if err != nil {
			b.Fatal(err)
		}
		if len(schema.Fields()) != 4 {
			b.Fatalf("expected 4 fields, got %d", len(schema.Fields()))
		}
	}
}

func BenchmarkSchemaFromComplexStruct(b *testing.B) {
	s := BenchComplexStruct{
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
		String:    "test",
		Bool:      true,
		Time:      time.Now(),
		ByteSlice: []byte("test"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := SchemaFromStruct(s)
		if err != nil {
			b.Fatal(err)
		}
		if len(schema.Fields()) != 14 {
			b.Fatalf("expected 14 fields, got %d", len(schema.Fields()))
		}
	}
}

func BenchmarkSchemaFromNestedStruct(b *testing.B) {
	s := BenchNestedStruct{
		ID: 1,
		Nested: BenchSimpleStruct{
			Int:    42,
			String: "test",
			Float:  3.14,
			Bool:   true,
		},
		Array: []int32{1, 2, 3},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := SchemaFromStruct(s)
		if err != nil {
			b.Fatal(err)
		}
		if len(schema.Fields()) != 3 {
			b.Fatalf("expected 3 fields, got %d", len(schema.Fields()))
		}
	}
}

// Benchmarks for inferring schema from maps
func BenchmarkSchemaFromMapSmall(b *testing.B) {
	m := generateMapData(5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := SchemaFromMap(m)
		if err != nil {
			b.Fatal(err)
		}
		if len(schema.Fields()) != 5 {
			b.Fatalf("expected 5 fields, got %d", len(schema.Fields()))
		}
	}
}

func BenchmarkSchemaFromMapMedium(b *testing.B) {
	m := generateMapData(20)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := SchemaFromMap(m)
		if err != nil {
			b.Fatal(err)
		}
		if len(schema.Fields()) != 20 {
			b.Fatalf("expected 20 fields, got %d", len(schema.Fields()))
		}
	}
}

func BenchmarkSchemaFromMapLarge(b *testing.B) {
	m := generateMapData(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := SchemaFromMap(m)
		if err != nil {
			b.Fatal(err)
		}
		if len(schema.Fields()) != 100 {
			b.Fatalf("expected 100 fields, got %d", len(schema.Fields()))
		}
	}
}

func BenchmarkSchemaFromNestedMapSmall(b *testing.B) {
	m := generateNestedMapData(5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := SchemaFromMap(m)
		if err != nil {
			b.Fatal(err)
		}
		if len(schema.Fields()) != 5 {
			b.Fatalf("expected 5 fields, got %d", len(schema.Fields()))
		}
	}
}

func BenchmarkSchemaFromNestedMapMedium(b *testing.B) {
	m := generateNestedMapData(20)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := SchemaFromMap(m)
		if err != nil {
			b.Fatal(err)
		}
		if len(schema.Fields()) != 5 {
			b.Fatalf("expected 5 fields, got %d", len(schema.Fields()))
		}
	}
}

func BenchmarkSchemaFromNestedMapLarge(b *testing.B) {
	m := generateNestedMapData(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := SchemaFromMap(m)
		if err != nil {
			b.Fatal(err)
		}
		if len(schema.Fields()) != 5 {
			b.Fatalf("expected 5 fields, got %d", len(schema.Fields()))
		}
	}
}

// Benchmarks with custom options
func BenchmarkSchemaWithOptionsDictDisabled(b *testing.B) {
	s := BenchComplexStruct{
		String: "test",
	}

	opts := &SchemaOptions{
		EnableDictionary: false,
		MaxNestingDepth:  10,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := SchemaFromStructWithOptions(s, opts)
		if err != nil {
			b.Fatal(err)
		}
		if len(schema.Fields()) != 14 {
			b.Fatalf("expected 14 fields, got %d", len(schema.Fields()))
		}
	}
}

func BenchmarkSchemaWithOptionsMaxDepth(b *testing.B) {
	// Create a nested struct that should exceed our depth limit
	s := BenchNestedStruct{
		ID: 1,
		Nested: BenchSimpleStruct{
			Int:    42,
			String: "test",
			Float:  3.14,
			Bool:   true,
		},
		Array: []int32{1, 2, 3},
	}

	opts := &SchemaOptions{
		EnableDictionary: true,
		MaxNestingDepth:  0, // Set to 0 to guarantee the error (nested struct has depth 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := SchemaFromStructWithOptions(s, opts)
		if err == nil {
			b.Fatal("expected error due to max nesting depth")
		}
	}
}

// Benchmark allocations
func BenchmarkSchemaAllocations(b *testing.B) {
	s := BenchComplexStruct{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = SchemaFromStruct(s)
	}
}
