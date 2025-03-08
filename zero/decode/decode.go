package decode

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/TFMV/arrowgen/internal/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Decoder decodes an Arrow Record into native Go types (structs or maps).
type Decoder struct {
	schema       *arrow.Schema
	memPool      *pool.MemPool
	structFields []cachedField // Cache struct field metadata.  Essential for performance.
	alloc        memory.Allocator
}

type cachedField struct {
	index     int          // Field index in the Go struct.
	fieldType reflect.Type // Type of the Go struct field.
	omitEmpty bool         // True if the "omitempty" tag is present.
	arrowName string       // Name of the Arrow field, considering tags.
}

// NewDecoder creates a new Decoder for the provided Arrow schema.
func NewDecoder(schema *arrow.Schema, opts ...Option) *Decoder {
	d := &Decoder{
		schema:  schema,
		memPool: pool.NewMemPool(),       //Consider removing memPool if zero-copy is crucial and the custom allocator is robust.
		alloc:   memory.DefaultAllocator, // Use a standard allocator by default
	}

	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Option represents a functional option for configuring the Decoder.
type Option func(*Decoder)

// WithAllocator provides a custom memory allocator for intermediate operations.
func WithAllocator(alloc memory.Allocator) Option {
	return func(d *Decoder) {
		d.alloc = alloc
	}
}

// Decode decodes an Arrow Record into the provided output pointer.
func (d *Decoder) Decode(record arrow.Record, out interface{}) error {
	// Check for nil output pointer first
	outVal := reflect.ValueOf(out)
	if outVal.Kind() != reflect.Ptr || outVal.IsNil() {
		return fmt.Errorf("out must be a non-nil pointer")
	}

	// Check if output is a pointer to slice
	elemType := outVal.Elem().Type()
	if elemType.Kind() != reflect.Slice {
		return fmt.Errorf("out must be a pointer to a slice")
	}

	// Check for nil record
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}

	// Check for nil schema
	if d.schema == nil {
		return fmt.Errorf("decoder schema cannot be nil")
	}

	sliceVal := outVal.Elem()
	numRows := int(record.NumRows())

	fmt.Println("Decoding record with", numRows, "rows and", record.NumCols(), "columns")
	fmt.Println("Output slice element type:", elemType.Elem().Name())

	// Zero-Copy:  Directly use the underlying data buffers.  No intermediate `values` slice.
	// Ensure output slice has enough capacity, re-use existing if possible, otherwise make slice
	if sliceVal.Cap() < numRows {
		sliceVal.Set(reflect.MakeSlice(sliceVal.Type(), numRows, numRows))
	} else {
		sliceVal.SetLen(numRows)
	}

	// Pre-initialize map elements and cache struct field information.
	isMap := elemType.Elem().Kind() == reflect.Map
	if isMap {
		mapType := elemType.Elem()
		for i := 0; i < numRows; i++ {
			elem := sliceVal.Index(i)
			if elem.IsNil() {
				elem.Set(reflect.MakeMap(mapType))
			}
		}
	} else {
		d.cacheStructFields(elemType.Elem()) // Cache only once per struct type.
	}

	numCols := int(record.NumCols())
	maxGoroutines := runtime.GOMAXPROCS(0)
	numGoroutines := min(maxGoroutines, numCols)
	if numRows < 1000 || isMap {
		numGoroutines = 1 // Single-threaded for small data sets or maps
	}

	fmt.Println("Using", numGoroutines, "goroutines for decoding")
	fmt.Println("Cached struct fields:", len(d.structFields))
	for i, field := range d.structFields {
		fmt.Printf("  Field %d: %s (Index: %d, Type: %v)\n", i+1, field.arrowName, field.index, field.fieldType)
	}

	workChan := make(chan int, numCols)
	errChan := make(chan error, numCols)
	var wg sync.WaitGroup

	// No row-level mutexes needed with zero-copy since we're not creating intermediate slices.

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for colIdx := range workChan {
				col := record.Column(colIdx)
				if col == nil {
					errChan <- fmt.Errorf("column %d is nil", colIdx)
					return
				}
				fieldName := record.Schema().Field(colIdx).Name
				var field cachedField
				if !isMap {
					for _, f := range d.structFields {
						if f.arrowName == fieldName {
							field = f
							break
						}
					}
					if field.index == 0 && field.fieldType == nil { // Check both for zero-value.
						continue // Field not found in struct, skip.  Don't error; allows schema evolution.
					}
				}

				// Directly decode into struct or map
				for rowIdx := 0; rowIdx < numRows; rowIdx++ {
					elem := sliceVal.Index(rowIdx)
					if isMap {
						if err := d.setMapValue(elem, fieldName, col, rowIdx); err != nil {
							errChan <- fmt.Errorf("failed to set map value for field %s at row %d: %w", fieldName, rowIdx, err)
							return
						}
					} else {
						if err := d.setStructFieldValue(elem, field, col, rowIdx); err != nil {
							errChan <- fmt.Errorf("failed to set struct field %s at row %d: %w", fieldName, rowIdx, err)
							return
						}
					}
				}
			}
		}()
	}

	for i := 0; i < numCols; i++ {
		workChan <- i
	}
	close(workChan)

	wg.Wait()
	close(errChan)

	for err := range errChan {
		return err
	}

	return nil
}

// cacheStructFields builds the cached field information.
func (d *Decoder) cacheStructFields(t reflect.Type) {
	if d.structFields != nil {
		return // Already cached.
	}

	numFields := t.NumField()
	d.structFields = make([]cachedField, 0, numFields) // Pre-allocate, but use append for flexibility.

	fmt.Println("Caching struct fields for type:", t.Name())
	for i := 0; i < numFields; i++ {
		f := t.Field(i)
		arrowName := f.Tag.Get("arrow")
		if arrowName == "" {
			arrowName = f.Name // Default to field name.
		}
		omitEmpty := false
		if tag, ok := f.Tag.Lookup("arrow"); ok {
			if tag == "-" || contains(tag, ",omitempty") {
				omitEmpty = true
			}
		}
		if arrowName != "-" { // Skip fields explicitly excluded with `arrow:"-"`
			fmt.Printf("  Field %d: %s -> %s (Type: %v)\n", i, f.Name, arrowName, f.Type)
			d.structFields = append(d.structFields, cachedField{
				index:     i,
				fieldType: f.Type,
				omitEmpty: omitEmpty,
				arrowName: arrowName,
			})
		}
	}
}

// setStructFieldValue sets a field on a struct, handling various types.
//
//	This version directly uses the Arrow array data, with NO intermediate allocations.
func (d *Decoder) setStructFieldValue(elem reflect.Value, field cachedField, col arrow.Array, rowIdx int) error {
	fmt.Printf("Setting field %s (index %d) for row %d\n", field.arrowName, field.index, rowIdx)

	if col.IsNull(rowIdx) {
		fmt.Println("  Value is null")
		if field.omitEmpty {
			return nil
		}
		// Set to zero value if the field is nil.
		elem.Field(field.index).Set(reflect.Zero(field.fieldType))
		return nil
	}

	// Get a settable field value
	fieldValue := elem.Field(field.index)
	if !fieldValue.CanSet() {
		return fmt.Errorf("field %s is not settable", field.arrowName)
	}

	fmt.Printf("  Column type: %T\n", col)

	switch arr := col.(type) {
	case *array.Int64:
		fmt.Printf("  Int64 value: %d\n", arr.Value(rowIdx))
		if fieldValue.Kind() == reflect.Struct && field.fieldType == reflect.TypeOf(time.Time{}) {
			// Special handling for time.Time
			t := time.Unix(0, arr.Value(rowIdx))
			fieldValue.Set(reflect.ValueOf(t))
		} else {
			fieldValue.SetInt(arr.Value(rowIdx))
		}
	case *array.Int8:
		fieldValue.SetInt(int64(arr.Value(rowIdx)))
	case *array.Int16:
		fieldValue.SetInt(int64(arr.Value(rowIdx)))
	case *array.Int32:
		fieldValue.SetInt(int64(arr.Value(rowIdx)))
	case *array.Uint8:
		fieldValue.SetUint(uint64(arr.Value(rowIdx)))
	case *array.Uint16:
		fieldValue.SetUint(uint64(arr.Value(rowIdx)))
	case *array.Uint32:
		fieldValue.SetUint(uint64(arr.Value(rowIdx)))
	case *array.Uint64:
		fieldValue.SetUint(arr.Value(rowIdx))
	case *array.Float32:
		fieldValue.SetFloat(float64(arr.Value(rowIdx)))
	case *array.Float64:
		fieldValue.SetFloat(arr.Value(rowIdx))
	case *array.String:
		val := arr.Value(rowIdx)
		fmt.Printf("  String value: %s\n", val)
		switch field.fieldType.Kind() {
		case reflect.String:
			fieldValue.SetString(val) // Direct string assignment.
		case reflect.Slice:
			if field.fieldType.Elem().Kind() == reflect.Uint8 { // Check for []byte
				fieldValue.SetBytes([]byte(val)) // Efficient []byte conversion
			} else {
				return fmt.Errorf("cannot assign string to field %s (type %v)", field.arrowName, field.fieldType)
			}
		default:
			return fmt.Errorf("cannot assign string to field %s (type %v)", field.arrowName, field.fieldType)
		}
	case *array.Boolean:
		fieldValue.SetBool(arr.Value(rowIdx))
	case *array.Timestamp:
		if field.fieldType == reflect.TypeOf(time.Time{}) {
			ts := arr.Value(rowIdx)
			var t time.Time
			switch arr.DataType().(*arrow.TimestampType).Unit {
			case arrow.Nanosecond:
				t = time.Unix(0, int64(ts))
			case arrow.Microsecond:
				t = time.Unix(0, int64(ts)*1000)
			case arrow.Millisecond:
				t = time.Unix(0, int64(ts)*1000000)
			case arrow.Second:
				t = time.Unix(int64(ts), 0)
			}
			fieldValue.Set(reflect.ValueOf(t))
		} else {
			fieldValue.SetInt(int64(arr.Value(rowIdx)))
		}
	case *array.Date32:
		if field.fieldType == reflect.TypeOf(time.Time{}) {
			days := int64(arr.Value(rowIdx))
			t := time.Unix(days*86400, 0)
			fieldValue.Set(reflect.ValueOf(t))
		} else {
			fieldValue.SetInt(int64(arr.Value(rowIdx)))
		}
	case *array.Date64:
		if field.fieldType == reflect.TypeOf(time.Time{}) {
			ms := int64(arr.Value(rowIdx))
			t := time.Unix(0, ms*1000000)
			fieldValue.Set(reflect.ValueOf(t))
		} else {
			fieldValue.SetInt(int64(arr.Value(rowIdx)))
		}
	case *array.Binary:
		val := arr.Value(rowIdx)
		switch field.fieldType.Kind() {
		case reflect.String:
			fieldValue.SetString(string(val))
		case reflect.Slice:
			if field.fieldType.Elem().Kind() == reflect.Uint8 {
				fieldValue.SetBytes(val)
			} else {
				return fmt.Errorf("cannot assign binary to field %s (type %v)", field.arrowName, field.fieldType)
			}
		default:
			return fmt.Errorf("cannot assign binary to field %s (type %v)", field.arrowName, field.fieldType)
		}
	case *array.Dictionary:
		fmt.Printf("  Dictionary array, row %d\n", rowIdx)
		if arr.IsNull(rowIdx) {
			fmt.Println("    Value is null")
			if field.omitEmpty {
				return nil
			}
			fieldValue.Set(reflect.Zero(field.fieldType))
			return nil
		}
		index := arr.GetValueIndex(rowIdx)
		fmt.Printf("    Value index: %d\n", index)
		switch dict := arr.Dictionary().(type) {
		case *array.String:
			val := dict.Value(index)
			fmt.Printf("    String value: %s\n", val)
			switch field.fieldType.Kind() {
			case reflect.String:
				fmt.Printf("    Setting string value: %s\n", val)
				fieldValue.SetString(val) // Direct string assignment.
			case reflect.Slice:
				if field.fieldType.Elem().Kind() == reflect.Uint8 { // Check for []byte
					fieldValue.SetBytes([]byte(val)) // Efficient []byte conversion

				} else {
					return fmt.Errorf("cannot assign string to field %s (type %v)", field.arrowName, field.fieldType)
				}
			default:
				return fmt.Errorf("cannot assign string to field %s (type %v)", field.arrowName, field.fieldType)
			}
		case *array.Binary:
			val := dict.Value(index)
			switch field.fieldType.Kind() {
			case reflect.String:
				fieldValue.SetString(string(val))
			case reflect.Slice:
				if field.fieldType.Elem().Kind() == reflect.Uint8 {
					fieldValue.SetBytes(val)
				} else {
					return fmt.Errorf("cannot assign binary to field %s (type %v)", field.arrowName, field.fieldType)
				}
			}
		default:
			return fmt.Errorf("unsupported dictionary type %T", dict)
		}
	default:
		return fmt.Errorf("unsupported array type: %T", col)
	}

	return nil
}

// setMapValue sets a value in a map, handling various types.
func (d *Decoder) setMapValue(mapVal reflect.Value, key string, col arrow.Array, rowIdx int) error {
	if col.IsNull(rowIdx) {
		return nil // Skip null values for maps
	}

	// Get the map's value type
	mapType := mapVal.Type()
	valueType := mapType.Elem()

	fmt.Printf("Setting map value for key %s, value type: %v\n", key, valueType)

	// Create a new value of the appropriate type
	var val reflect.Value

	switch arr := col.(type) {
	case *array.Int64:
		if valueType.Kind() == reflect.Struct && valueType == reflect.TypeOf(time.Time{}) {
			// Special handling for time.Time
			t := time.Unix(0, arr.Value(rowIdx))
			val = reflect.ValueOf(t)
		} else if valueType.Kind() == reflect.Interface {
			// For interface{}, use the concrete type directly
			val = reflect.ValueOf(arr.Value(rowIdx))
		} else if canConvertToType(valueType, reflect.Int64) {
			val = reflect.ValueOf(arr.Value(rowIdx)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert int64 to map value type %v", valueType)
		}
	case *array.Int8:
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(int8(arr.Value(rowIdx)))
		} else if canConvertToType(valueType, reflect.Int8) {
			val = reflect.ValueOf(arr.Value(rowIdx)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert int8 to map value type %v", valueType)
		}
	case *array.Int16:
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(int16(arr.Value(rowIdx)))
		} else if canConvertToType(valueType, reflect.Int16) {
			val = reflect.ValueOf(arr.Value(rowIdx)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert int16 to map value type %v", valueType)
		}
	case *array.Int32:
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(int32(arr.Value(rowIdx)))
		} else if canConvertToType(valueType, reflect.Int32) {
			val = reflect.ValueOf(arr.Value(rowIdx)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert int32 to map value type %v", valueType)
		}
	case *array.Uint8:
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(uint8(arr.Value(rowIdx)))
		} else if canConvertToType(valueType, reflect.Uint8) {
			val = reflect.ValueOf(arr.Value(rowIdx)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert uint8 to map value type %v", valueType)
		}
	case *array.Uint16:
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(uint16(arr.Value(rowIdx)))
		} else if canConvertToType(valueType, reflect.Uint16) {
			val = reflect.ValueOf(arr.Value(rowIdx)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert uint16 to map value type %v", valueType)
		}
	case *array.Uint32:
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(uint32(arr.Value(rowIdx)))
		} else if canConvertToType(valueType, reflect.Uint32) {
			val = reflect.ValueOf(arr.Value(rowIdx)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert uint32 to map value type %v", valueType)
		}
	case *array.Uint64:
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(arr.Value(rowIdx))
		} else if canConvertToType(valueType, reflect.Uint64) {
			val = reflect.ValueOf(arr.Value(rowIdx)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert uint64 to map value type %v", valueType)
		}
	case *array.Float32:
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(arr.Value(rowIdx))
		} else if canConvertToType(valueType, reflect.Float32) {
			val = reflect.ValueOf(arr.Value(rowIdx)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert float32 to map value type %v", valueType)
		}
	case *array.Float64:
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(arr.Value(rowIdx))
		} else if canConvertToType(valueType, reflect.Float64) {
			val = reflect.ValueOf(arr.Value(rowIdx)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert float64 to map value type %v", valueType)
		}
	case *array.String:
		strVal := arr.Value(rowIdx)
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(strVal)
		} else if valueType.Kind() == reflect.String {
			val = reflect.ValueOf(strVal)
		} else if valueType.Kind() == reflect.Slice && valueType.Elem().Kind() == reflect.Uint8 {
			val = reflect.ValueOf([]byte(strVal))
		} else {
			return fmt.Errorf("cannot convert string to map value type %v", valueType)
		}
	case *array.Boolean:
		if valueType.Kind() == reflect.Interface {
			val = reflect.ValueOf(arr.Value(rowIdx))
		} else if valueType.Kind() == reflect.Bool {
			val = reflect.ValueOf(arr.Value(rowIdx))
		} else {
			return fmt.Errorf("cannot convert bool to map value type %v", valueType)
		}
	case *array.Timestamp:
		ts := arr.Value(rowIdx)
		if valueType.Kind() == reflect.Interface {
			// For interface{}, use time.Time
			var t time.Time
			switch arr.DataType().(*arrow.TimestampType).Unit {
			case arrow.Nanosecond:
				t = time.Unix(0, int64(ts))
			case arrow.Microsecond:
				t = time.Unix(0, int64(ts)*1000)
			case arrow.Millisecond:
				t = time.Unix(0, int64(ts)*1000000)
			case arrow.Second:
				t = time.Unix(int64(ts), 0)
			}
			val = reflect.ValueOf(t)
		} else if valueType == reflect.TypeOf(time.Time{}) {
			var t time.Time
			switch arr.DataType().(*arrow.TimestampType).Unit {
			case arrow.Nanosecond:
				t = time.Unix(0, int64(ts))
			case arrow.Microsecond:
				t = time.Unix(0, int64(ts)*1000)
			case arrow.Millisecond:
				t = time.Unix(0, int64(ts)*1000000)
			case arrow.Second:
				t = time.Unix(int64(ts), 0)
			}
			val = reflect.ValueOf(t)
		} else if canConvertToType(valueType, reflect.Int64) {
			val = reflect.ValueOf(int64(ts)).Convert(valueType)
		} else {
			return fmt.Errorf("cannot convert timestamp to map value type %v", valueType)
		}
	case *array.Dictionary:
		index := arr.GetValueIndex(rowIdx)
		switch dict := arr.Dictionary().(type) {
		case *array.String:
			strVal := dict.Value(index)
			if valueType.Kind() == reflect.Interface {
				val = reflect.ValueOf(strVal)
			} else if valueType.Kind() == reflect.String {
				val = reflect.ValueOf(strVal)
			} else if valueType.Kind() == reflect.Slice && valueType.Elem().Kind() == reflect.Uint8 {
				val = reflect.ValueOf([]byte(strVal))
			} else {
				return fmt.Errorf("cannot convert dictionary string to map value type %v", valueType)
			}
		case *array.Binary:
			binVal := dict.Value(index)
			if valueType.Kind() == reflect.Interface {
				// For interface{}, prefer string over []byte
				val = reflect.ValueOf(string(binVal))
			} else if valueType.Kind() == reflect.String {
				val = reflect.ValueOf(string(binVal))
			} else if valueType.Kind() == reflect.Slice && valueType.Elem().Kind() == reflect.Uint8 {
				val = reflect.ValueOf(binVal)
			} else {
				return fmt.Errorf("cannot convert dictionary binary to map value type %v", valueType)
			}
		default:
			return fmt.Errorf("unsupported dictionary type %T", dict)
		}
	default:
		return fmt.Errorf("unsupported array type: %T", col)
	}

	// Set the map value
	fmt.Printf("  Setting map value: %v (type: %T)\n", val.Interface(), val.Interface())
	mapVal.SetMapIndex(reflect.ValueOf(key), val)
	return nil
}

// Helper function to check if a value can be converted to a given type
func canConvertToType(destType reflect.Type, srcKind reflect.Kind) bool {
	// Special case for interface{} - can accept any type
	if destType.Kind() == reflect.Interface {
		return true
	}

	switch destType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return srcKind == reflect.Int8 || srcKind == reflect.Int16 ||
			srcKind == reflect.Int32 || srcKind == reflect.Int64
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return srcKind == reflect.Uint8 || srcKind == reflect.Uint16 ||
			srcKind == reflect.Uint32 || srcKind == reflect.Uint64
	case reflect.Float32, reflect.Float64:
		return srcKind == reflect.Float32 || srcKind == reflect.Float64
	default:
		return destType.Kind() == srcKind
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr
}

// Helper function to get the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Unsafe string to []byte conversion.  Use with caution.  The resulting
// byte slice's underlying array MUST NOT be modified.
func stringToBytesUnsafe(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// Unsafe []byte to string conversion.
func bytesToStringUnsafe(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
