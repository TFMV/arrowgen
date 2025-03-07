package decode

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

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
	Bytes     []byte    `arrow:"bytes"`
	Bool      bool      `arrow:"bool"`
	Time      time.Time `arrow:"time"`
	OmitEmpty string    `arrow:"omit,omitempty"`
}

type CustomTags struct {
	ID        int64  `arrow:"-"`           // Should be ignored
	Name      string `arrow:"custom_name"` // Custom field name
	Value     string `arrow:",omitempty"`  // Use field name, but omit if empty
	Untag     string // Should use field name
	OmitEmpty string `arrow:"omit,omitempty"` // Custom name and omit if empty
}

func TestDecoder_AllTypes(t *testing.T) {
	// Create schema
	fields := []arrow.Field{
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
		{Name: "bytes", Type: arrow.BinaryTypes.Binary},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}},
		{Name: "omit", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create test data
	pool := memory.NewGoAllocator()
	now := time.Now().UTC()

	// Create builders
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
	binaryBuilder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	boolBuilder := array.NewBooleanBuilder(pool)
	timeBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Nanosecond})
	omitBuilder := array.NewStringBuilder(pool)

	// Append values
	int8Builder.Append(42)
	int16Builder.Append(4242)
	int32Builder.Append(424242)
	int64Builder.Append(42424242)
	uint8Builder.Append(42)
	uint16Builder.Append(4242)
	uint32Builder.Append(424242)
	uint64Builder.Append(42424242)
	float32Builder.Append(42.42)
	float64Builder.Append(4242.4242)
	stringBuilder.Append("test")
	binaryBuilder.Append([]byte("binary"))
	boolBuilder.Append(true)
	timeBuilder.Append(arrow.Timestamp(now.UnixNano()))
	omitBuilder.AppendNull()

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
		binaryBuilder.NewArray(),
		boolBuilder.NewArray(),
		timeBuilder.NewArray(),
		omitBuilder.NewArray(),
	}

	// Create record
	record := array.NewRecord(schema, cols, 1)
	defer record.Release()

	// Create decoder and decode
	decoder := NewDecoder(schema)
	var result []AllTypes
	err := decoder.Decode(record, &result)
	assert.NoError(t, err)

	// Verify results
	assert.Len(t, result, 1)
	assert.Equal(t, int8(42), result[0].Int8)
	assert.Equal(t, int16(4242), result[0].Int16)
	assert.Equal(t, int32(424242), result[0].Int32)
	assert.Equal(t, int64(42424242), result[0].Int64)
	assert.Equal(t, uint8(42), result[0].Uint8)
	assert.Equal(t, uint16(4242), result[0].Uint16)
	assert.Equal(t, uint32(424242), result[0].Uint32)
	assert.Equal(t, uint64(42424242), result[0].Uint64)
	assert.Equal(t, float32(42.42), result[0].Float32)
	assert.Equal(t, float64(4242.4242), result[0].Float64)
	assert.Equal(t, "test", result[0].String)
	assert.Equal(t, []byte("binary"), result[0].Bytes)
	assert.Equal(t, true, result[0].Bool)
	assert.Equal(t, now.Unix(), result[0].Time.Unix())
	assert.Equal(t, "", result[0].OmitEmpty) // Should be empty due to null value and omitempty
}

func TestDecoder_Map(t *testing.T) {
	// Create schema
	fields := []arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int64},
		{Name: "str", Type: arrow.BinaryTypes.String},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create test data
	pool := memory.NewGoAllocator()

	// Create builders
	intBuilder := array.NewInt64Builder(pool)
	strBuilder := array.NewStringBuilder(pool)
	boolBuilder := array.NewBooleanBuilder(pool)

	// Append values
	intBuilder.Append(42)
	strBuilder.Append("test")
	boolBuilder.Append(true)

	// Create arrays
	cols := []arrow.Array{
		intBuilder.NewArray(),
		strBuilder.NewArray(),
		boolBuilder.NewArray(),
	}

	// Create record
	record := array.NewRecord(schema, cols, 1)
	defer record.Release()

	// Create decoder and decode
	decoder := NewDecoder(schema)
	var result []map[string]interface{}
	err := decoder.Decode(record, &result)
	assert.NoError(t, err)

	// Verify results
	assert.Len(t, result, 1)
	assert.Equal(t, int64(42), result[0]["int"])
	assert.Equal(t, "test", result[0]["str"])
	assert.Equal(t, true, result[0]["bool"])
}

func TestDecoder_CustomTags(t *testing.T) {
	// Create schema
	fields := []arrow.Field{
		{Name: "custom_name", Type: arrow.BinaryTypes.String},
		{Name: "Value", Type: arrow.BinaryTypes.String},
		{Name: "Untag", Type: arrow.BinaryTypes.String},
		{Name: "omit", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create test data
	pool := memory.NewGoAllocator()

	// Create builders
	nameBuilder := array.NewStringBuilder(pool)
	valueBuilder := array.NewStringBuilder(pool)
	untagBuilder := array.NewStringBuilder(pool)
	omitBuilder := array.NewStringBuilder(pool)

	// Append values
	nameBuilder.Append("name")
	valueBuilder.AppendNull() // Test omitempty
	untagBuilder.Append("untag")
	omitBuilder.AppendNull()

	// Create arrays
	cols := []arrow.Array{
		nameBuilder.NewArray(),
		valueBuilder.NewArray(),
		untagBuilder.NewArray(),
		omitBuilder.NewArray(),
	}

	// Create record
	record := array.NewRecord(schema, cols, 1)
	defer record.Release()

	// Create decoder and decode
	decoder := NewDecoder(schema)
	var result []CustomTags
	err := decoder.Decode(record, &result)
	assert.NoError(t, err)

	// Verify results
	assert.Len(t, result, 1)
	assert.Equal(t, int64(0), result[0].ID) // Should be zero (ignored)
	assert.Equal(t, "name", result[0].Name)
	assert.Equal(t, "", result[0].Value) // Should be empty due to null + omitempty
	assert.Equal(t, "untag", result[0].Untag)
	assert.Equal(t, "", result[0].OmitEmpty) // Should be empty due to null + omitempty
}

func TestDecoder_DictionaryEncoding(t *testing.T) {
	// Create schema with dictionary-encoded string
	fields := []arrow.Field{
		{Name: "dict_str", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create test data
	pool := memory.NewGoAllocator()

	// Create dictionary builder
	dictBuilder := array.NewDictionaryBuilder(pool, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String})
	strDictBuilder := dictBuilder.(*array.BinaryDictionaryBuilder)

	// Add values
	strDictBuilder.Append([]byte("test1"))
	strDictBuilder.Append([]byte("test2"))
	strDictBuilder.AppendNull()
	strDictBuilder.Append([]byte("test1")) // Reuse dictionary value

	// Create record
	record := array.NewRecord(schema, []arrow.Array{strDictBuilder.NewArray()}, 4)
	defer record.Release()

	// Create decoder and decode
	decoder := NewDecoder(schema)
	var result []struct {
		DictStr string `arrow:"dict_str"`
	}
	err := decoder.Decode(record, &result)
	assert.NoError(t, err)

	// Verify results
	assert.Len(t, result, 4)
	assert.Equal(t, "test1", result[0].DictStr)
	assert.Equal(t, "test2", result[1].DictStr)
	assert.Equal(t, "", result[2].DictStr)
	assert.Equal(t, "test1", result[3].DictStr)
}

func TestDecoder_Errors(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "test", Type: arrow.BinaryTypes.String}}, nil)
	decoder := NewDecoder(schema)

	t.Run("nil pointer", func(t *testing.T) {
		err := decoder.Decode(nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be a non-nil pointer")
	})

	t.Run("non-slice pointer", func(t *testing.T) {
		var x int
		err := decoder.Decode(nil, &x)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be a pointer to a slice")
	})

	t.Run("nil record", func(t *testing.T) {
		var result []struct{ Test string }
		err := decoder.Decode(nil, &result)
		assert.Error(t, err)
	})
}

func TestDecoder_WithAllocator(t *testing.T) {
	customAllocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "test", Type: arrow.BinaryTypes.String}}, nil)

	decoder := NewDecoder(schema, WithAllocator(customAllocator))
	assert.Equal(t, customAllocator, decoder.alloc)
}
