package encode

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Mode represents the operational mode of the encoder.
type Mode uint8

const (
	// ModeZeroAlloc optimizes for minimal allocations and GC impact.
	ModeZeroAlloc Mode = iota
	// ModeHighThroughput optimizes for maximum processing speed.
	ModeHighThroughput
)

// Encoder encodes Go types into Arrow records.
type Encoder struct {
	schema  *arrow.Schema
	mode    Mode
	pool    memory.Allocator
	workers int // Number of worker goroutines for high-throughput mode
	cache   sync.Map
}

// Option configures the encoder.
type Option func(*Encoder)

// WithMode sets the operational mode.
func WithMode(mode Mode) Option {
	return func(e *Encoder) {
		e.mode = mode
	}
}

// WithAllocator sets a custom memory allocator.
func WithAllocator(pool memory.Allocator) Option {
	return func(e *Encoder) {
		e.pool = pool
	}
}

// WithWorkers sets the number of worker goroutines for high-throughput mode.
func WithWorkers(n int) Option {
	return func(e *Encoder) {
		e.workers = n
	}
}

// NewEncoder creates a new encoder instance.
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

// Encode converts a slice of Go values into an Arrow record.
func (e *Encoder) Encode(data interface{}) (arrow.Record, error) {
	val := reflect.ValueOf(data)
	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("data must be a slice, got %v", val.Kind())
	}

	numRows := val.Len()
	if numRows == 0 {
		return e.createEmptyRecord()
	}

	// Create builders for each field in the schema
	builders := make([]array.Builder, len(e.schema.Fields()))
	for i, field := range e.schema.Fields() {
		builders[i] = array.NewBuilder(e.pool, field.Type)
	}

	// Encode the data based on the selected mode
	var err error
	if e.mode == ModeHighThroughput {
		err = e.encodeHighThroughput(val, builders)
	} else {
		err = e.encodeZeroAlloc(val, builders)
	}

	// If there's an error, release builders and return
	if err != nil {
		for _, builder := range builders {
			if builder != nil {
				builder.Release()
			}
		}
		return nil, err
	}

	// Create arrays from builders
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		// Release builder after creating array
		builder.Release()
	}

	// Create record from arrays
	record := array.NewRecord(e.schema, arrays, int64(numRows))

	// Release arrays after creating the record
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}

func (e *Encoder) createEmptyRecord() (arrow.Record, error) {
	cols := make([]arrow.Array, len(e.schema.Fields()))
	for i, field := range e.schema.Fields() {
		builder := array.NewBuilder(e.pool, field.Type)
		cols[i] = builder.NewArray()
		builder.Release()
	}

	record := array.NewRecord(e.schema, cols, 0)

	// Release arrays after creating the record
	for _, arr := range cols {
		arr.Release()
	}

	return record, nil
}

func (e *Encoder) encodeZeroAlloc(val reflect.Value, builders []array.Builder) error {
	numRows := val.Len()
	elemType := val.Type().Elem()

	// Handle pointer to struct slices
	if elemType.Kind() == reflect.Ptr && elemType.Elem().Kind() == reflect.Struct {
		for j, builder := range builders {
			field := e.schema.Field(j)
			for i := 0; i < numRows; i++ {
				elem := val.Index(i)
				// Handle nil pointers
				if elem.IsNil() {
					builder.AppendNull()
					continue
				}

				// Dereference the pointer
				structElem := elem.Elem()
				if err := e.encodeStructField(structElem, field.Name, builder); err != nil {
					return fmt.Errorf("failed to encode struct field %s at row %d: %v", field.Name, i, err)
				}
			}
		}
		return nil
	}

	// Handle struct slices
	if elemType.Kind() == reflect.Struct {
		for j, builder := range builders {
			field := e.schema.Field(j)
			for i := 0; i < numRows; i++ {
				elem := val.Index(i)
				if err := e.encodeStructField(elem, field.Name, builder); err != nil {
					return fmt.Errorf("failed to encode struct field %s at row %d: %v", field.Name, i, err)
				}
			}
		}
		return nil
	}

	// Handle map slices
	if elemType.Kind() == reflect.Map {
		for j, builder := range builders {
			field := e.schema.Field(j)
			for i := 0; i < numRows; i++ {
				elem := val.Index(i)
				if err := e.encodeMapField(elem, field.Name, builder); err != nil {
					return fmt.Errorf("failed to encode map field %s at row %d: %v", field.Name, i, err)
				}
			}
		}
		return nil
	}

	return fmt.Errorf("unsupported slice element type: %v", elemType)
}

func (e *Encoder) encodeHighThroughput(val reflect.Value, builders []array.Builder) error {
	numRows := val.Len()
	elemType := val.Type().Elem()

	// For small datasets, use zero-alloc mode.
	if numRows < 1000 {
		return e.encodeZeroAlloc(val, builders)
	}

	// Reserve capacity for all builders to avoid resizing during parallel processing
	for _, builder := range builders {
		// Reserve capacity with some extra room to avoid resizing
		if reservable, ok := builder.(interface{ Reserve(int) }); ok {
			reservable.Reserve(numRows)
		}
	}

	// Create mutexes to protect builders during parallel access
	builderMutexes := make([]sync.Mutex, len(builders))

	// Handle struct slices with parallel processing.
	if elemType.Kind() == reflect.Struct {
		errChan := make(chan error, e.workers)
		var wg sync.WaitGroup

		// Process rows in parallel, but ensure each row is processed completely
		rowsPerWorker := (numRows + e.workers - 1) / e.workers // Ceiling division

		for w := 0; w < e.workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				// Calculate the range of rows this worker will process
				startRow := workerID * rowsPerWorker
				endRow := startRow + rowsPerWorker
				if endRow > numRows {
					endRow = numRows
				}

				// Process each row in the worker's range
				for i := startRow; i < endRow; i++ {
					elem := val.Index(i)

					// Process all fields for this row
					for j := range builders {
						field := e.schema.Field(j)

						// Lock the builder before accessing it
						builderMutexes[j].Lock()
						err := e.encodeStructField(elem, field.Name, builders[j])
						builderMutexes[j].Unlock()

						if err != nil {
							select {
							case errChan <- fmt.Errorf("failed to encode struct field %s at row %d: %v", field.Name, i, err):
							default:
							}
							return
						}
					}
				}
			}(w)
		}

		wg.Wait()
		close(errChan)

		// Check for errors
		select {
		case err := <-errChan:
			return err
		default:
			// No errors
		}

		return nil
	}

	// Handle map slices with parallel processing.
	if elemType.Kind() == reflect.Map {
		errChan := make(chan error, e.workers)
		var wg sync.WaitGroup

		// Process rows in parallel, but ensure each row is processed completely
		rowsPerWorker := (numRows + e.workers - 1) / e.workers // Ceiling division

		for w := 0; w < e.workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				// Calculate the range of rows this worker will process
				startRow := workerID * rowsPerWorker
				endRow := startRow + rowsPerWorker
				if endRow > numRows {
					endRow = numRows
				}

				// Process each row in the worker's range
				for i := startRow; i < endRow; i++ {
					elem := val.Index(i)

					// Process all fields for this row
					for j := range builders {
						field := e.schema.Field(j)

						// Lock the builder before accessing it
						builderMutexes[j].Lock()
						err := e.encodeMapField(elem, field.Name, builders[j])
						builderMutexes[j].Unlock()

						if err != nil {
							select {
							case errChan <- fmt.Errorf("failed to encode map field %s at row %d: %v", field.Name, i, err):
							default:
							}
							return
						}
					}
				}
			}(w)
		}

		wg.Wait()
		close(errChan)

		// Check for errors
		select {
		case err := <-errChan:
			return err
		default:
			// No errors
		}

		return nil
	}

	return fmt.Errorf("unsupported slice element type: %v", elemType)
}

// encodeStructField looks up the field by matching the "arrow" tag (or field name if no tag)
// and then appends its value using the given builder.
func (e *Encoder) encodeStructField(val reflect.Value, fieldName string, builder array.Builder) error {
	t := val.Type()
	var fieldValue reflect.Value
	found := false

	// First try to find the field by the exact name
	field, ok := t.FieldByName(fieldName)
	if ok {
		fieldValue = val.FieldByIndex(field.Index)
		found = true
	}

	// If not found, try case-insensitive matching
	if !found {
		// Try camelCase and PascalCase variations
		camelCaseName := toCamelCase(fieldName)
		pascalCaseName := toPascalCase(fieldName)

		// Iterate through all fields to find a match
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)

			// Check field name
			if strings.EqualFold(field.Name, fieldName) ||
				field.Name == camelCaseName ||
				field.Name == pascalCaseName {
				fieldValue = val.Field(i)
				found = true
				break
			}

			// Check tag
			tag := field.Tag.Get("arrow")
			if tag == fieldName {
				fieldValue = val.Field(i)
				found = true
				break
			}
		}
	}

	if !found {
		// Field not found, append null
		builder.AppendNull()
		return nil
	}

	// Append the field value
	return e.appendValue(builder, fieldValue)
}

// toCamelCase converts a snake_case string to camelCase
func toCamelCase(s string) string {
	// Split the string by underscore
	parts := strings.Split(s, "_")

	// Capitalize the first letter of each part except the first one
	for i := 1; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}

	// Join the parts back together
	return strings.Join(parts, "")
}

// toPascalCase converts a snake_case string to PascalCase
func toPascalCase(s string) string {
	// Split the string by underscore
	parts := strings.Split(s, "_")

	// Capitalize the first letter of each part
	for i := 0; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}

	// Join the parts back together
	return strings.Join(parts, "")
}

func (e *Encoder) encodeMapField(val reflect.Value, fieldName string, builder array.Builder) error {
	if val.Kind() != reflect.Map {
		return fmt.Errorf("expected map, got %v", val.Kind())
	}

	// Get the value for the field
	mapValue := val.MapIndex(reflect.ValueOf(fieldName))
	if !mapValue.IsValid() {
		// Field not found in map, append null
		builder.AppendNull()
		return nil
	}

	// Append the value
	return e.appendValue(builder, mapValue)
}

func (e *Encoder) appendValue(builder array.Builder, val reflect.Value) error {
	if !val.IsValid() {
		builder.AppendNull()
		return nil
	}

	// For interface{} values from maps, get the concrete value.
	if val.Kind() == reflect.Interface {
		val = val.Elem()
	}
	if !val.IsValid() {
		builder.AppendNull()
		return nil
	}

	// Handle nil pointers.
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			builder.AppendNull()
			return nil
		}
		val = val.Elem()
	}

	// Type-specific handling.
	b := builder
	switch b := b.(type) {
	case *array.Int8Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(int8(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(int8(val.Uint()))
		case reflect.Bool:
			if val.Bool() {
				b.Append(1)
			} else {
				b.Append(0)
			}
		default:
			b.AppendNull()
		}
	case *array.Int16Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(int16(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(int16(val.Uint()))
		default:
			b.AppendNull()
		}
	case *array.Int32Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(int32(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(int32(val.Uint()))
		default:
			b.AppendNull()
		}
	case *array.Int64Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(val.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(int64(val.Uint()))
		default:
			b.AppendNull()
		}
	case *array.Uint8Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(uint8(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(uint8(val.Uint()))
		case reflect.Bool:
			if val.Bool() {
				b.Append(1)
			} else {
				b.Append(0)
			}
		default:
			b.AppendNull()
		}
	case *array.Uint16Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(uint16(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(uint16(val.Uint()))
		default:
			b.AppendNull()
		}
	case *array.Uint32Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(uint32(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(uint32(val.Uint()))
		default:
			b.AppendNull()
		}
	case *array.Uint64Builder:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(uint64(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(val.Uint())
		default:
			b.AppendNull()
		}
	case *array.Float32Builder:
		switch val.Kind() {
		case reflect.Float32, reflect.Float64:
			b.Append(float32(val.Float()))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(float32(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(float32(val.Uint()))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch val.Kind() {
		case reflect.Float32, reflect.Float64:
			b.Append(val.Float())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.Append(float64(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.Append(float64(val.Uint()))
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
			// Convert time.Time to timestamp safely
			timeVal := val.Interface().(time.Time)
			timestamp := arrow.Timestamp(timeVal.UnixNano())
			b.Append(timestamp)
		case val.Kind() == reflect.Int64:
			b.Append(arrow.Timestamp(val.Int()))
		default:
			b.AppendNull()
		}
	case *array.ListBuilder:
		// Handle slice fields
		if val.Kind() == reflect.Slice {
			listBuilder := b
			listBuilder.Append(true) // Start a new list
			valueBuilder := listBuilder.ValueBuilder()

			// Append each element in the slice
			for i := 0; i < val.Len(); i++ {
				elemValue := val.Index(i)

				// Handle different value builder types
				switch vb := valueBuilder.(type) {
				case *array.StringBuilder:
					// Handle string elements
					if elemValue.Kind() == reflect.String {
						vb.Append(elemValue.String())
					} else {
						vb.AppendNull()
					}
				case *array.Int64Builder:
					// Handle int64 elements
					if elemValue.Kind() == reflect.Int64 || elemValue.Kind() == reflect.Int {
						vb.Append(elemValue.Int())
					} else {
						vb.AppendNull()
					}
				case *array.Float64Builder:
					// Handle float64 elements
					if elemValue.Kind() == reflect.Float64 || elemValue.Kind() == reflect.Float32 {
						vb.Append(elemValue.Float())
					} else {
						vb.AppendNull()
					}
				case *array.BooleanBuilder:
					// Handle boolean elements
					if elemValue.Kind() == reflect.Bool {
						vb.Append(elemValue.Bool())
					} else {
						vb.AppendNull()
					}
				default:
					// For other types, try the generic approach
					if err := e.appendValue(valueBuilder, elemValue); err != nil {
						return fmt.Errorf("failed to append list element at index %d: %v", i, err)
					}
				}
			}
		} else {
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
	case *array.MapBuilder:
		// Handle map fields
		if val.Kind() == reflect.Map {
			mapBuilder := b
			mapBuilder.Append(true) // Start a new map

			// Get the key and value builders
			keyBuilder := mapBuilder.KeyBuilder()
			itemBuilder := mapBuilder.ItemBuilder()

			// Iterate through the map and append each key-value pair
			iter := val.MapRange()
			for iter.Next() {
				k := iter.Key()
				v := iter.Value()

				// Handle string keys (most common case)
				if k.Kind() == reflect.String {
					// For dictionary-encoded string keys
					if dictBuilder, ok := keyBuilder.(*array.BinaryDictionaryBuilder); ok {
						if err := dictBuilder.Append([]byte(k.String())); err != nil {
							return fmt.Errorf("failed to append map key: %v", err)
						}
					} else if strBuilder, ok := keyBuilder.(*array.StringBuilder); ok {
						strBuilder.Append(k.String())
					} else {
						return fmt.Errorf("expected string builder for map keys, got %T", keyBuilder)
					}
				} else {
					// For other key types
					if err := e.appendValue(keyBuilder, k); err != nil {
						return fmt.Errorf("failed to append map key: %v", err)
					}
				}

				// Append the value
				if err := e.appendValue(itemBuilder, v); err != nil {
					return fmt.Errorf("failed to append map value: %v", err)
				}
			}
		} else {
			b.AppendNull()
		}
	case array.DictionaryBuilder:
		switch val.Kind() {
		case reflect.String:
			// For dictionary-encoded string types
			if binaryDict, ok := b.(*array.BinaryDictionaryBuilder); ok {
				if err := binaryDict.Append([]byte(val.String())); err != nil {
					return fmt.Errorf("failed to append string to binary dictionary builder: %v", err)
				}
			} else {
				// Fallback to appending null
				b.AppendNull()
			}
		default:
			b.AppendNull()
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
	return nil
}

// stringToBytesUnsafe performs zero-copy conversion from string to []byte.
func stringToBytesUnsafe(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
