package encode

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/TFMV/arrowgen/internal/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Encoder encodes slices of native Go types (structs or maps)
// into an Apache Arrow Record based on a given schema.
type Encoder struct {
	schema    *arrow.Schema
	memPool   *pool.MemPool
	valuePool *pool.ValuePool
	simdPool  *pool.SIMDPool
	fieldMap  sync.Map // Thread-safe map for field indices
}

// NewEncoder creates a new Encoder instance with the provided Arrow schema.
func NewEncoder(schema *arrow.Schema) *Encoder {
	return &Encoder{
		schema:    schema,
		memPool:   pool.NewMemPool(),
		valuePool: pool.NewValuePool(),
		simdPool:  pool.NewSIMDPool(),
	}
}

// Encode accepts a slice of structs or maps and encodes it into an Arrow Record.
func (e *Encoder) Encode(data interface{}) (arrow.Record, error) {
	val := reflect.ValueOf(data)
	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("data must be a slice")
	}

	numRows := val.Len()
	if numRows == 0 {
		// Create empty record with schema
		cols := make([]arrow.Array, len(e.schema.Fields()))
		for i, field := range e.schema.Fields() {
			builder, err := makeBuilder(e.memPool.Allocator(), field.Type)
			if err != nil {
				return nil, err
			}
			defer builder.Release()
			cols[i] = builder.NewArray()
		}
		return array.NewRecord(e.schema, cols, 0), nil
	}

	// Create builders for each field
	builders := make([]array.Builder, len(e.schema.Fields()))
	defer func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}()

	// Initialize builders
	for i, field := range e.schema.Fields() {
		var err error
		builders[i], err = makeBuilder(e.memPool.Allocator(), field.Type)
		if err != nil {
			return nil, err
		}
	}

	// Determine optimal number of goroutines based on data size
	numFields := len(e.schema.Fields())
	maxGoroutines := runtime.GOMAXPROCS(0)
	var numGoroutines int
	if numRows < 1000 {
		// For small datasets, process sequentially
		numGoroutines = 1
	} else if numRows < 10000 {
		// For medium datasets, use half the available processors
		numGoroutines = maxGoroutines / 2
	} else {
		// For large datasets, use all available processors
		numGoroutines = maxGoroutines
	}

	// Limit goroutines to number of fields
	if numGoroutines > numFields {
		numGoroutines = numFields
	}

	// Create work channel and error channel
	workChan := make(chan int, numFields)
	errChan := make(chan error, numFields)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fieldIdx := range workChan {
				field := e.schema.Field(fieldIdx)
				builder := builders[fieldIdx]

				// Extract values for this field
				values := make([]interface{}, 0, numRows)
				if err := e.extractValues(val, field.Name, &values, numRows); err != nil {
					errChan <- fmt.Errorf("failed to extract values for field %s: %w", field.Name, err)
					return
				}

				// Append values to builder
				if err := e.appendValues(builder, values); err != nil {
					errChan <- fmt.Errorf("failed to append values for field %s: %w", field.Name, err)
					return
				}

				// Return values slice to pool
				e.valuePool.Put(values)
			}
		}()
	}

	// Send work to goroutines
	for i := range e.schema.Fields() {
		workChan <- i
	}
	close(workChan)

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	// Create arrays from builders
	cols := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		cols[i] = builder.NewArray()
	}

	return array.NewRecord(e.schema, cols, int64(numRows)), nil
}

// extractValues extracts values from a slice of structs or maps for a given field
func (e *Encoder) extractValues(v reflect.Value, fieldName string, values *[]interface{}, numRows int) error {
	// Get slice from pool
	*values = e.valuePool.Get()
	if cap(*values) < numRows {
		// If pooled slice is too small, create a new one
		*values = make([]interface{}, 0, numRows)
	} else {
		*values = (*values)[:0] // Reset length but keep capacity
	}

	isStruct := v.Index(0).Kind() == reflect.Struct

	if isStruct {
		// Cache field index for struct access
		fieldIdxI, ok := e.fieldMap.Load(fieldName)
		if !ok {
			// Find and cache the field index
			t := v.Index(0).Type()
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				name := field.Tag.Get("arrow")
				if name == "" {
					name = field.Name
				}
				if name == fieldName {
					e.fieldMap.Store(fieldName, i)
					fieldIdxI = i
					break
				}
			}
			if _, ok := e.fieldMap.Load(fieldName); !ok {
				return fmt.Errorf("field %s not found in struct", fieldName)
			}
		}

		fieldIdx := fieldIdxI.(int)
		// Extract values using cached field index
		for i := 0; i < numRows; i++ {
			*values = append(*values, v.Index(i).Field(fieldIdx).Interface())
		}
	} else {
		// Handle map type
		for i := 0; i < numRows; i++ {
			mapVal := v.Index(i)
			val := mapVal.MapIndex(reflect.ValueOf(fieldName))
			if !val.IsValid() {
				*values = append(*values, nil)
			} else {
				*values = append(*values, val.Interface())
			}
		}
	}

	return nil
}

// appendValues appends values to a builder with SIMD optimizations where possible
func (e *Encoder) appendValues(builder array.Builder, values []interface{}) error {
	// Fast path for numeric types using SIMD
	if len(values) >= 8 { // Only use SIMD for larger slices
		switch b := builder.(type) {
		case *array.Int8Builder:
			return appendInt8ValuesSIMD(e, b, values)
		case *array.Int16Builder:
			return appendInt16ValuesSIMD(b, values)
		case *array.Int32Builder:
			return appendInt32ValuesSIMD(b, values)
		case *array.Int64Builder:
			return appendInt64ValuesSIMD(b, values)
		case *array.Uint8Builder:
			return appendUint8ValuesSIMD(b, values)
		case *array.Uint16Builder:
			return appendUint16ValuesSIMD(b, values)
		case *array.Uint32Builder:
			return appendUint32ValuesSIMD(b, values)
		case *array.Uint64Builder:
			return appendUint64ValuesSIMD(b, values)
		case *array.Float32Builder:
			return appendFloat32ValuesSIMD(b, values)
		case *array.Float64Builder:
			return appendFloat64ValuesSIMD(b, values)
		}
	}

	// Regular path for other types or small slices
	switch b := builder.(type) {
	case *array.BinaryDictionaryBuilder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case string:
					if err := b.Append([]byte(val)); err != nil {
						return err
					}
				case *string:
					if val != nil {
						if err := b.Append([]byte(*val)); err != nil {
							return err
						}
					} else {
						b.AppendNull()
					}
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Int8Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case int8:
					b.Append(val)
				case int:
					b.Append(int8(val))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Int16Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case int16:
					b.Append(val)
				case int:
					b.Append(int16(val))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Int32Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case int32:
					b.Append(val)
				case int:
					b.Append(int32(val))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Int64Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case int64:
					b.Append(val)
				case int:
					b.Append(int64(val))
				case time.Time:
					b.Append(val.UnixNano())
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Uint8Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case uint8:
					b.Append(val)
				case uint:
					b.Append(uint8(val))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Uint16Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case uint16:
					b.Append(val)
				case uint:
					b.Append(uint16(val))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Uint32Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case uint32:
					b.Append(val)
				case uint:
					b.Append(uint32(val))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Uint64Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case uint64:
					b.Append(val)
				case uint:
					b.Append(uint64(val))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Float32Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case float32:
					b.Append(val)
				case float64:
					b.Append(float32(val))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Float64Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case float64:
					b.Append(val)
				case float32:
					b.Append(float64(val))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.StringBuilder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case string:
					b.Append(val)
				case *string:
					if val != nil {
						b.Append(*val)
					} else {
						b.AppendNull()
					}
				default:
					b.AppendNull()
				}
			}
		}
	case *array.BooleanBuilder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case bool:
					b.Append(val)
				case *bool:
					if val != nil {
						b.Append(*val)
					} else {
						b.AppendNull()
					}
				default:
					b.AppendNull()
				}
			}
		}
	case *array.TimestampBuilder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case int64:
					b.Append(arrow.Timestamp(val))
				case time.Time:
					b.Append(arrow.Timestamp(val.UnixNano()))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Date32Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case int32:
					b.Append(arrow.Date32(val))
				case time.Time:
					b.Append(arrow.Date32(val.Unix() / 86400))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Date64Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case int64:
					b.Append(arrow.Date64(val))
				case time.Time:
					b.Append(arrow.Date64(val.UnixMilli()))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Time32Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case int32:
					b.Append(arrow.Time32(val))
				case time.Time:
					b.Append(arrow.Time32(val.Unix()))
				default:
					b.AppendNull()
				}
			}
		}
	case *array.Time64Builder:
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				switch val := v.(type) {
				case int64:
					b.Append(arrow.Time64(val))
				case time.Time:
					b.Append(arrow.Time64(val.UnixNano()))
				default:
					b.AppendNull()
				}
			}
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
	return nil
}

// appendInt8ValuesSIMD appends int8 values using SIMD-style processing
func appendInt8ValuesSIMD(e *Encoder, b *array.Int8Builder, values []interface{}) error {
	data := e.simdPool.GetInt8Slice(len(values))
	nulls := e.simdPool.GetBoolSlice(len(values))
	defer func() {
		e.simdPool.PutInt8Slice(data)
		e.simdPool.PutBoolSlice(nulls)
	}()

	// Process values in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if values[i+j] == nil {
				nulls[i+j] = true
			} else if val, ok := toInt8(values[i+j]); ok {
				data[i+j] = val
			} else {
				nulls[i+j] = true
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if values[i] == nil {
			nulls[i] = true
		} else if val, ok := toInt8(values[i]); ok {
			data[i] = val
		} else {
			nulls[i] = true
		}
	}

	// Bulk append to builder
	for i := 0; i < len(values); i++ {
		if nulls[i] {
			b.AppendNull()
		} else {
			b.Append(data[i])
		}
	}

	return nil
}

// appendInt16ValuesSIMD appends int16 values using SIMD-style processing
func appendInt16ValuesSIMD(b *array.Int16Builder, values []interface{}) error {
	data := make([]int16, len(values))
	nulls := make([]bool, len(values))

	// Process values in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if values[i+j] == nil {
				nulls[i+j] = true
			} else if val, ok := toInt16(values[i+j]); ok {
				data[i+j] = val
			} else {
				nulls[i+j] = true
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if values[i] == nil {
			nulls[i] = true
		} else if val, ok := toInt16(values[i]); ok {
			data[i] = val
		} else {
			nulls[i] = true
		}
	}

	// Bulk append to builder
	for i := 0; i < len(values); i++ {
		if nulls[i] {
			b.AppendNull()
		} else {
			b.Append(data[i])
		}
	}

	return nil
}

// appendInt32ValuesSIMD appends int32 values using SIMD-style processing
func appendInt32ValuesSIMD(b *array.Int32Builder, values []interface{}) error {
	data := make([]int32, len(values))
	nulls := make([]bool, len(values))

	// Process values in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if values[i+j] == nil {
				nulls[i+j] = true
			} else if val, ok := toInt32(values[i+j]); ok {
				data[i+j] = val
			} else {
				nulls[i+j] = true
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if values[i] == nil {
			nulls[i] = true
		} else if val, ok := toInt32(values[i]); ok {
			data[i] = val
		} else {
			nulls[i] = true
		}
	}

	// Bulk append to builder
	for i := 0; i < len(values); i++ {
		if nulls[i] {
			b.AppendNull()
		} else {
			b.Append(data[i])
		}
	}

	return nil
}

// appendInt64ValuesSIMD appends int64 values using SIMD-style processing
func appendInt64ValuesSIMD(b *array.Int64Builder, values []interface{}) error {
	data := make([]int64, len(values))
	nulls := make([]bool, len(values))

	// Process values in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if values[i+j] == nil {
				nulls[i+j] = true
			} else if val, ok := toInt64(values[i+j]); ok {
				data[i+j] = val
			} else {
				nulls[i+j] = true
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if values[i] == nil {
			nulls[i] = true
		} else if val, ok := toInt64(values[i]); ok {
			data[i] = val
		} else {
			nulls[i] = true
		}
	}

	// Bulk append to builder
	for i := 0; i < len(values); i++ {
		if nulls[i] {
			b.AppendNull()
		} else {
			b.Append(data[i])
		}
	}

	return nil
}

// appendUint8ValuesSIMD appends uint8 values using SIMD-style processing
func appendUint8ValuesSIMD(b *array.Uint8Builder, values []interface{}) error {
	data := make([]uint8, len(values))
	nulls := make([]bool, len(values))

	// Process values in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if values[i+j] == nil {
				nulls[i+j] = true
			} else if val, ok := toUint8(values[i+j]); ok {
				data[i+j] = val
			} else {
				nulls[i+j] = true
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if values[i] == nil {
			nulls[i] = true
		} else if val, ok := toUint8(values[i]); ok {
			data[i] = val
		} else {
			nulls[i] = true
		}
	}

	// Bulk append to builder
	for i := 0; i < len(values); i++ {
		if nulls[i] {
			b.AppendNull()
		} else {
			b.Append(data[i])
		}
	}

	return nil
}

// appendUint16ValuesSIMD appends uint16 values using SIMD-style processing
func appendUint16ValuesSIMD(b *array.Uint16Builder, values []interface{}) error {
	data := make([]uint16, len(values))
	nulls := make([]bool, len(values))

	// Process values in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if values[i+j] == nil {
				nulls[i+j] = true
			} else if val, ok := toUint16(values[i+j]); ok {
				data[i+j] = val
			} else {
				nulls[i+j] = true
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if values[i] == nil {
			nulls[i] = true
		} else if val, ok := toUint16(values[i]); ok {
			data[i] = val
		} else {
			nulls[i] = true
		}
	}

	// Bulk append to builder
	for i := 0; i < len(values); i++ {
		if nulls[i] {
			b.AppendNull()
		} else {
			b.Append(data[i])
		}
	}

	return nil
}

// appendUint32ValuesSIMD appends uint32 values using SIMD-style processing
func appendUint32ValuesSIMD(b *array.Uint32Builder, values []interface{}) error {
	data := make([]uint32, len(values))
	nulls := make([]bool, len(values))

	// Process values in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if values[i+j] == nil {
				nulls[i+j] = true
			} else if val, ok := toUint32(values[i+j]); ok {
				data[i+j] = val
			} else {
				nulls[i+j] = true
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if values[i] == nil {
			nulls[i] = true
		} else if val, ok := toUint32(values[i]); ok {
			data[i] = val
		} else {
			nulls[i] = true
		}
	}

	// Bulk append to builder
	for i := 0; i < len(values); i++ {
		if nulls[i] {
			b.AppendNull()
		} else {
			b.Append(data[i])
		}
	}

	return nil
}

// appendUint64ValuesSIMD appends uint64 values using SIMD-style processing
func appendUint64ValuesSIMD(b *array.Uint64Builder, values []interface{}) error {
	data := make([]uint64, len(values))
	nulls := make([]bool, len(values))

	// Process values in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if values[i+j] == nil {
				nulls[i+j] = true
			} else if val, ok := toUint64(values[i+j]); ok {
				data[i+j] = val
			} else {
				nulls[i+j] = true
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if values[i] == nil {
			nulls[i] = true
		} else if val, ok := toUint64(values[i]); ok {
			data[i] = val
		} else {
			nulls[i] = true
		}
	}

	// Bulk append to builder
	for i := 0; i < len(values); i++ {
		if nulls[i] {
			b.AppendNull()
		} else {
			b.Append(data[i])
		}
	}

	return nil
}

// appendFloat32ValuesSIMD appends float32 values using SIMD-style processing
func appendFloat32ValuesSIMD(b *array.Float32Builder, values []interface{}) error {
	data := make([]float32, len(values))
	nulls := make([]bool, len(values))

	// Process values in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if values[i+j] == nil {
				nulls[i+j] = true
			} else if val, ok := toFloat32(values[i+j]); ok {
				data[i+j] = val
			} else {
				nulls[i+j] = true
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if values[i] == nil {
			nulls[i] = true
		} else if val, ok := toFloat32(values[i]); ok {
			data[i] = val
		} else {
			nulls[i] = true
		}
	}

	// Bulk append to builder
	for i := 0; i < len(values); i++ {
		if nulls[i] {
			b.AppendNull()
		} else {
			b.Append(data[i])
		}
	}

	return nil
}

// appendFloat64ValuesSIMD appends float64 values using SIMD-style processing
func appendFloat64ValuesSIMD(b *array.Float64Builder, values []interface{}) error {
	data := make([]float64, len(values))
	nulls := make([]bool, len(values))

	// Process values in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if values[i+j] == nil {
				nulls[i+j] = true
			} else if val, ok := toFloat64(values[i+j]); ok {
				data[i+j] = val
			} else {
				nulls[i+j] = true
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if values[i] == nil {
			nulls[i] = true
		} else if val, ok := toFloat64(values[i]); ok {
			data[i] = val
		} else {
			nulls[i] = true
		}
	}

	// Bulk append to builder
	for i := 0; i < len(values); i++ {
		if nulls[i] {
			b.AppendNull()
		} else {
			b.Append(data[i])
		}
	}

	return nil
}

// Keep the original type conversion functions
func toInt8(val interface{}) (int8, bool) {
	switch v := val.(type) {
	case int8:
		return v, true
	case int:
		return int8(v), true
	default:
		return 0, false
	}
}

func toInt16(val interface{}) (int16, bool) {
	switch v := val.(type) {
	case int16:
		return v, true
	case int:
		return int16(v), true
	default:
		return 0, false
	}
}

func toInt32(val interface{}) (int32, bool) {
	switch v := val.(type) {
	case int32:
		return v, true
	case int:
		return int32(v), true
	default:
		return 0, false
	}
}

func toInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case time.Time:
		return v.UnixNano(), true
	default:
		return 0, false
	}
}

func toUint8(val interface{}) (uint8, bool) {
	switch v := val.(type) {
	case uint8:
		return v, true
	case uint:
		return uint8(v), true
	default:
		return 0, false
	}
}

func toUint16(val interface{}) (uint16, bool) {
	switch v := val.(type) {
	case uint16:
		return v, true
	case uint:
		return uint16(v), true
	default:
		return 0, false
	}
}

func toUint32(val interface{}) (uint32, bool) {
	switch v := val.(type) {
	case uint32:
		return v, true
	case uint:
		return uint32(v), true
	default:
		return 0, false
	}
}

func toUint64(val interface{}) (uint64, bool) {
	switch v := val.(type) {
	case uint64:
		return v, true
	case uint:
		return uint64(v), true
	default:
		return 0, false
	}
}

func toFloat32(val interface{}) (float32, bool) {
	switch v := val.(type) {
	case float32:
		return v, true
	case float64:
		return float32(v), true
	default:
		return 0, false
	}
}

func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// makeBuilder creates an Arrow builder for a given Arrow data type.
func makeBuilder(pool memory.Allocator, dt arrow.DataType) (array.Builder, error) {
	switch t := dt.(type) {
	case *arrow.DictionaryType:
		dictBuilder := array.NewDictionaryBuilder(pool, t)
		return dictBuilder, nil
	case *arrow.Int8Type:
		return array.NewInt8Builder(pool), nil
	case *arrow.Int16Type:
		return array.NewInt16Builder(pool), nil
	case *arrow.Int32Type:
		return array.NewInt32Builder(pool), nil
	case *arrow.Int64Type:
		return array.NewInt64Builder(pool), nil
	case *arrow.Uint8Type:
		return array.NewUint8Builder(pool), nil
	case *arrow.Uint16Type:
		return array.NewUint16Builder(pool), nil
	case *arrow.Uint32Type:
		return array.NewUint32Builder(pool), nil
	case *arrow.Uint64Type:
		return array.NewUint64Builder(pool), nil
	case *arrow.Float32Type:
		return array.NewFloat32Builder(pool), nil
	case *arrow.Float64Type:
		return array.NewFloat64Builder(pool), nil
	case *arrow.StringType:
		return array.NewStringBuilder(pool), nil
	case *arrow.BooleanType:
		return array.NewBooleanBuilder(pool), nil
	case *arrow.TimestampType:
		return array.NewTimestampBuilder(pool, t), nil
	case *arrow.Date32Type:
		return array.NewDate32Builder(pool), nil
	case *arrow.Date64Type:
		return array.NewDate64Builder(pool), nil
	case *arrow.Time32Type:
		return array.NewTime32Builder(pool, t), nil
	case *arrow.Time64Type:
		return array.NewTime64Builder(pool, t), nil
	default:
		return nil, fmt.Errorf("unsupported Arrow type: %v", dt)
	}
}
