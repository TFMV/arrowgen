package decode

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	Int8      int8    `arrow:"int8"`
	Int16     int16   `arrow:"int16"`
	Int32     int32   `arrow:"int32"`
	Int64     int64   `arrow:"int64"`
	Uint8     uint8   `arrow:"uint8"`
	Uint16    uint16  `arrow:"uint16"`
	Uint32    uint32  `arrow:"uint32"`
	Uint64    uint64  `arrow:"uint64"`
	Float32   float32 `arrow:"float32"`
	Float64   float64 `arrow:"float64"`
	String    string  `arrow:"string"`
	Boolean   bool    `arrow:"boolean"`
	Timestamp int64   `arrow:"timestamp"`
	Date32    int32   `arrow:"date32"`
	Date64    int64   `arrow:"date64"`
	Time32    int32   `arrow:"time32"`
	Time64    int64   `arrow:"time64"`
}

func TestDecoder_AllTypes(t *testing.T) {
	// Create schema with all supported types
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
		{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{Name: "date32", Type: arrow.FixedWidthTypes.Date32},
		{Name: "date64", Type: arrow.FixedWidthTypes.Date64},
		{Name: "time32", Type: arrow.FixedWidthTypes.Time32s},
		{Name: "time64", Type: arrow.FixedWidthTypes.Time64ns},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create test data
	now := time.Now()
	expected := TestStruct{
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
		String:    "test",
		Boolean:   true,
		Timestamp: now.UnixNano(),
		Date32:    int32(now.Unix() / 86400),
		Date64:    now.UnixMilli(),
		Time32:    int32(now.Unix()),
		Time64:    now.UnixNano(),
	}

	// Create Arrow arrays
	pool := memory.NewGoAllocator()
	builders := make([]array.Builder, len(fields))
	arrays := make([]arrow.Array, len(fields))

	// Create and fill builders
	builders[0] = array.NewInt8Builder(pool)
	builders[0].(*array.Int8Builder).Append(expected.Int8)
	builders[1] = array.NewInt16Builder(pool)
	builders[1].(*array.Int16Builder).Append(expected.Int16)
	builders[2] = array.NewInt32Builder(pool)
	builders[2].(*array.Int32Builder).Append(expected.Int32)
	builders[3] = array.NewInt64Builder(pool)
	builders[3].(*array.Int64Builder).Append(expected.Int64)
	builders[4] = array.NewUint8Builder(pool)
	builders[4].(*array.Uint8Builder).Append(expected.Uint8)
	builders[5] = array.NewUint16Builder(pool)
	builders[5].(*array.Uint16Builder).Append(expected.Uint16)
	builders[6] = array.NewUint32Builder(pool)
	builders[6].(*array.Uint32Builder).Append(expected.Uint32)
	builders[7] = array.NewUint64Builder(pool)
	builders[7].(*array.Uint64Builder).Append(expected.Uint64)
	builders[8] = array.NewFloat32Builder(pool)
	builders[8].(*array.Float32Builder).Append(expected.Float32)
	builders[9] = array.NewFloat64Builder(pool)
	builders[9].(*array.Float64Builder).Append(expected.Float64)
	builders[10] = array.NewStringBuilder(pool)
	builders[10].(*array.StringBuilder).Append(expected.String)
	builders[11] = array.NewBooleanBuilder(pool)
	builders[11].(*array.BooleanBuilder).Append(expected.Boolean)
	builders[12] = array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"})
	builders[12].(*array.TimestampBuilder).Append(arrow.Timestamp(expected.Timestamp))
	builders[13] = array.NewDate32Builder(pool)
	builders[13].(*array.Date32Builder).Append(arrow.Date32(expected.Date32))
	builders[14] = array.NewDate64Builder(pool)
	builders[14].(*array.Date64Builder).Append(arrow.Date64(expected.Date64))
	builders[15] = array.NewTime32Builder(pool, &arrow.Time32Type{Unit: arrow.Second})
	builders[15].(*array.Time32Builder).Append(arrow.Time32(expected.Time32))
	builders[16] = array.NewTime64Builder(pool, &arrow.Time64Type{Unit: arrow.Nanosecond})
	builders[16].(*array.Time64Builder).Append(arrow.Time64(expected.Time64))

	// Build arrays
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		builder.Release()
	}

	// Create record
	record := array.NewRecord(schema, arrays, 1)
	defer record.Release()

	// Decode
	var result []TestStruct
	decoder := NewDecoder(schema)
	err := decoder.Decode(record, &result)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, expected, result[0])
}

func TestDecoder_NullValues(t *testing.T) {
	fields := []arrow.Field{
		{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "string", Type: arrow.BinaryTypes.String},
		{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean},
	}
	schema := arrow.NewSchema(fields, nil)

	type TestStruct struct {
		Int64   *int64  `arrow:"int64"`
		String  *string `arrow:"string"`
		Boolean *bool   `arrow:"boolean"`
	}

	// Create Arrow arrays with null values
	pool := memory.NewGoAllocator()
	builders := make([]array.Builder, len(fields))
	arrays := make([]arrow.Array, len(fields))

	builders[0] = array.NewInt64Builder(pool)
	builders[0].(*array.Int64Builder).AppendNull()
	builders[1] = array.NewStringBuilder(pool)
	builders[1].(*array.StringBuilder).AppendNull()
	builders[2] = array.NewBooleanBuilder(pool)
	builders[2].(*array.BooleanBuilder).AppendNull()

	// Build arrays
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		builder.Release()
	}

	// Create record
	record := array.NewRecord(schema, arrays, 1)
	defer record.Release()

	// Decode
	var result []TestStruct
	decoder := NewDecoder(schema)
	err := decoder.Decode(record, &result)
	assert.NoError(t, err)
	assert.Len(t, result, 1)

	// Verify null values
	assert.Nil(t, result[0].Int64)
	assert.Nil(t, result[0].String)
	assert.Nil(t, result[0].Boolean)
}

func TestDecoder_MapOutput(t *testing.T) {
	fields := []arrow.Field{
		{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "string", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create Arrow arrays
	pool := memory.NewGoAllocator()
	builders := make([]array.Builder, len(fields))
	arrays := make([]arrow.Array, len(fields))

	builders[0] = array.NewInt64Builder(pool)
	builders[0].(*array.Int64Builder).Append(42)
	builders[1] = array.NewStringBuilder(pool)
	builders[1].(*array.StringBuilder).Append("test")

	// Build arrays
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		builder.Release()
	}

	// Create record
	record := array.NewRecord(schema, arrays, 1)
	defer record.Release()

	// Decode into map
	var result []map[string]interface{}
	decoder := NewDecoder(schema)
	err := decoder.Decode(record, &result)
	assert.NoError(t, err)
	assert.Len(t, result, 1)

	// Verify values
	assert.Equal(t, int64(42), result[0]["int64"])
	assert.Equal(t, "test", result[0]["string"])
}
