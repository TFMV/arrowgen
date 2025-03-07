package encode

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Mode represents the operational mode of the encoder
type Mode uint8

const (
	// ModeZeroAlloc optimizes for minimal allocations and GC impact
	ModeZeroAlloc Mode = iota
	// ModeHighThroughput optimizes for maximum processing speed
	ModeHighThroughput
)

// Encoder encodes Go types into Arrow records
type Encoder struct {
	schema  *arrow.Schema
	mode    Mode
	pool    memory.Allocator
	workers int // Number of worker goroutines for high-throughput mode
	cache   sync.Map
}

// Option configures the encoder
type Option func(*Encoder)

// WithMode sets the operational mode
func WithMode(mode Mode) Option {
	return func(e *Encoder) {
		e.mode = mode
	}
}

// WithAllocator sets a custom memory allocator
func WithAllocator(pool memory.Allocator) Option {
	return func(e *Encoder) {
		e.pool = pool
	}
}

// WithWorkers sets the number of worker goroutines for high-throughput mode
func WithWorkers(n int) Option {
	return func(e *Encoder) {
		e.workers = n
	}
}

// NewEncoder creates a new encoder instance
func NewEncoder(schema *arrow.Schema, opts ...Option) *Encoder {
	e := &Encoder{
		schema:  schema,
		pool:    memory.DefaultAllocator,
		workers: runtime.GOMAXPROCS(0),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Encode converts a slice of Go values into an Arrow record
func (e *Encoder) Encode(data interface{}) (arrow.Record, error) {
	val := reflect.ValueOf(data)
	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("data must be a slice")
	}

	numRows := val.Len()
	if numRows == 0 {
		return e.createEmptyRecord()
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
		builders[i] = array.NewBuilder(e.pool, field.Type)
	}

	// Choose encoding strategy based on mode
	var err error
	switch e.mode {
	case ModeZeroAlloc:
		err = e.encodeZeroAlloc(val, builders)
	case ModeHighThroughput:
		err = e.encodeHighThroughput(val, builders)
	default:
		err = fmt.Errorf("invalid encoder mode: %d", e.mode)
	}
	if err != nil {
		return nil, err
	}

	// Create arrays from builders
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}

	return array.NewRecord(e.schema, arrays, int64(numRows)), nil
}

func (e *Encoder) createEmptyRecord() (arrow.Record, error) {
	arrays := make([]arrow.Array, len(e.schema.Fields()))
	for i, field := range e.schema.Fields() {
		builder := array.NewBuilder(e.pool, field.Type)
		defer builder.Release()
		arrays[i] = builder.NewArray()
	}
	return array.NewRecord(e.schema, arrays, 0), nil
}

func (e *Encoder) encodeZeroAlloc(val reflect.Value, builders []array.Builder) error {
	numRows := val.Len()
	elem := val.Index(0)
	isStruct := elem.Kind() == reflect.Struct

	// Process each field sequentially to minimize allocations
	for i, field := range e.schema.Fields() {
		builder := builders[i]
		if isStruct {
			if err := e.encodeStructField(val, field.Name, builder, numRows); err != nil {
				return fmt.Errorf("failed to encode struct field %s: %w", field.Name, err)
			}
		} else {
			if err := e.encodeMapField(val, field.Name, builder, numRows); err != nil {
				return fmt.Errorf("failed to encode map field %s: %w", field.Name, err)
			}
		}
	}

	return nil
}

func (e *Encoder) encodeHighThroughput(val reflect.Value, builders []array.Builder) error {
	numRows := val.Len()
	numFields := len(e.schema.Fields())
	workChan := make(chan int, numFields)
	errChan := make(chan error, numFields)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < e.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fieldIdx := range workChan {
				field := e.schema.Field(fieldIdx)
				builder := builders[fieldIdx]

				if err := e.encodeFieldConcurrent(val, field.Name, builder, numRows); err != nil {
					errChan <- fmt.Errorf("failed to encode field %s: %w", field.Name, err)
					return
				}
			}
		}()
	}

	// Distribute work
	for i := 0; i < numFields; i++ {
		workChan <- i
	}
	close(workChan)

	// Wait for completion
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) encodeStructField(val reflect.Value, fieldName string, builder array.Builder, numRows int) error {
	// Get cached field index
	fieldIdxI, ok := e.cache.Load(fieldName)
	if !ok {
		// Find and cache the field index
		t := val.Index(0).Type()
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			name := field.Tag.Get("arrow")
			if name == "" {
				name = field.Name
			}
			if name == fieldName {
				e.cache.Store(fieldName, i)
				fieldIdxI = i
				break
			}
		}
		if _, ok := e.cache.Load(fieldName); !ok {
			return fmt.Errorf("field %s not found in struct", fieldName)
		}
	}

	fieldIdx := fieldIdxI.(int)
	for i := 0; i < numRows; i++ {
		if err := e.appendValue(builder, val.Index(i).Field(fieldIdx)); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) encodeMapField(val reflect.Value, fieldName string, builder array.Builder, numRows int) error {
	for i := 0; i < numRows; i++ {
		mapVal := val.Index(i)
		fieldVal := mapVal.MapIndex(reflect.ValueOf(fieldName))
		if !fieldVal.IsValid() {
			builder.AppendNull()
			continue
		}
		if err := e.appendValue(builder, fieldVal); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) encodeFieldConcurrent(val reflect.Value, fieldName string, builder array.Builder, numRows int) error {
	isStruct := val.Index(0).Kind() == reflect.Struct
	if isStruct {
		return e.encodeStructField(val, fieldName, builder, numRows)
	}
	return e.encodeMapField(val, fieldName, builder, numRows)
}

func (e *Encoder) appendValue(builder array.Builder, val reflect.Value) error {
	if !val.IsValid() {
		builder.AppendNull()
		return nil
	}

	// For interface{} values from maps, we need to get the concrete value
	if val.Kind() == reflect.Interface {
		val = val.Elem()
	}

	switch b := builder.(type) {
	case *array.Int8Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(int8(val.Int()))
		default:
			b.AppendNull()
		}
	case *array.Int16Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(int16(val.Int()))
		default:
			b.AppendNull()
		}
	case *array.Int32Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(int32(val.Int()))
		default:
			b.AppendNull()
		}
	case *array.Int64Builder:
		switch {
		case val.Type() == reflect.TypeOf(time.Time{}):
			b.Append(val.Interface().(time.Time).UnixNano())
		case val.Kind() == reflect.Int || val.Kind() == reflect.Int8 ||
			val.Kind() == reflect.Int16 || val.Kind() == reflect.Int32 ||
			val.Kind() == reflect.Int64:
			b.Append(val.Int())
		default:
			b.AppendNull()
		}
	case *array.Uint8Builder:
		switch val.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(uint8(val.Uint()))
		default:
			b.AppendNull()
		}
	case *array.Uint16Builder:
		switch val.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(uint16(val.Uint()))
		default:
			b.AppendNull()
		}
	case *array.Uint32Builder:
		switch val.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(uint32(val.Uint()))
		default:
			b.AppendNull()
		}
	case *array.Uint64Builder:
		switch val.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(val.Uint())
		default:
			b.AppendNull()
		}
	case *array.Float32Builder:
		switch val.Kind() {
		case reflect.Float32, reflect.Float64:
			b.Append(float32(val.Float()))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch val.Kind() {
		case reflect.Float32, reflect.Float64:
			b.Append(val.Float())
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		switch val.Kind() {
		case reflect.String:
			b.Append(val.String())
		default:
			b.AppendNull()
		}
	case *array.BooleanBuilder:
		switch val.Kind() {
		case reflect.Bool:
			b.Append(val.Bool())
		default:
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		switch {
		case val.Type() == reflect.TypeOf(time.Time{}):
			b.Append(arrow.Timestamp(val.Interface().(time.Time).UnixNano()))
		case val.Kind() == reflect.Int64:
			b.Append(arrow.Timestamp(val.Int()))
		default:
			b.AppendNull()
		}
	case *array.BinaryBuilder:
		switch {
		case val.Kind() == reflect.String:
			b.Append(stringToBytesUnsafe(val.String()))
		case val.Kind() == reflect.Slice && val.Type().Elem().Kind() == reflect.Uint8:
			b.Append(val.Bytes())
		default:
			b.AppendNull()
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
	return nil
}

// stringToBytesUnsafe performs zero-copy conversion from string to []byte
func stringToBytesUnsafe(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
