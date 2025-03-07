package encode

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

func TestEncoder_AllTypes(t *testing.T) {
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
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: "date32", Type: arrow.FixedWidthTypes.Date32},
		{Name: "date64", Type: arrow.FixedWidthTypes.Date64},
		{Name: "time32", Type: arrow.FixedWidthTypes.Time32s},
		{Name: "time64", Type: arrow.FixedWidthTypes.Time64ns},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create test data
	now := time.Now()
	testData := []TestStruct{
		{
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
		},
	}

	// Create encoder and encode data
	enc := NewEncoder(schema)
	record, err := enc.Encode(testData)
	assert.NoError(t, err)
	defer record.Release()

	// Verify each column
	assert.Equal(t, int64(1), record.NumRows())
	assert.Equal(t, int64(17), record.NumCols())

	// Test each column type
	checkColumn(t, record.Column(0), int8(42))
	checkColumn(t, record.Column(1), int16(4242))
	checkColumn(t, record.Column(2), int32(424242))
	checkColumn(t, record.Column(3), int64(42424242))
	checkColumn(t, record.Column(4), uint8(42))
	checkColumn(t, record.Column(5), uint16(4242))
	checkColumn(t, record.Column(6), uint32(424242))
	checkColumn(t, record.Column(7), uint64(42424242))
	checkColumn(t, record.Column(8), float32(42.42))
	checkColumn(t, record.Column(9), float64(4242.4242))
	checkColumn(t, record.Column(10), "test")
	checkColumn(t, record.Column(11), true)
	checkColumn(t, record.Column(12), arrow.Timestamp(now.UnixNano()))
	checkColumn(t, record.Column(13), arrow.Date32(now.Unix()/86400))
	checkColumn(t, record.Column(14), arrow.Date64(now.UnixMilli()))
	checkColumn(t, record.Column(15), arrow.Time32(now.Unix()))
	checkColumn(t, record.Column(16), arrow.Time64(now.UnixNano()))
}

func checkColumn[T any](t *testing.T, col arrow.Array, expected T) {
	assert.NotNil(t, col)
	switch arr := col.(type) {
	case *array.Int8:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Int16:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Int32:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Int64:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Uint8:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Uint16:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Uint32:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Uint64:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Float32:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Float64:
		assert.Equal(t, expected, arr.Value(0))
	case *array.String:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Boolean:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Timestamp:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Date32:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Date64:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Time32:
		assert.Equal(t, expected, arr.Value(0))
	case *array.Time64:
		assert.Equal(t, expected, arr.Value(0))
	default:
		t.Errorf("unsupported array type: %T", col)
	}
}

func TestEncoder_NullValues(t *testing.T) {
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

	// Create test data with null values
	var nullInt64 *int64
	var nullString *string
	var nullBoolean *bool
	testData := []TestStruct{
		{
			Int64:   nullInt64,
			String:  nullString,
			Boolean: nullBoolean,
		},
	}

	enc := NewEncoder(schema)
	record, err := enc.Encode(testData)
	assert.NoError(t, err)
	defer record.Release()

	// Verify null values
	assert.Equal(t, int64(1), record.NumRows())
	assert.Equal(t, int64(3), record.NumCols())

	// Check that all values are null
	for i := 0; i < 3; i++ {
		assert.True(t, record.Column(i).IsNull(0))
	}
}
