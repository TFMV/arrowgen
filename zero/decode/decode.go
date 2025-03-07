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
	if col.IsNull(rowIdx) {
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

	switch arr := col.(type) {
	case *array.Int64:
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
	case *array.Time32:
		fieldValue.SetInt(int64(arr.Value(rowIdx)))
	case *array.Time64:
		fieldValue.SetInt(int64(arr.Value(rowIdx)))
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
		}
	case *array.Dictionary:
		if arr.IsNull(rowIdx) {
			if field.omitEmpty {
				return nil
			}
			fieldValue.Set(reflect.Zero(field.fieldType))
			return nil
		}
		index := arr.GetValueIndex(rowIdx)
		switch dict := arr.Dictionary().(type) {
		case *array.String:
			val := dict.Value(index)
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
		case *array.Binary:
			val := dict.Value(index)
			switch field.fieldType.Kind() {
			case reflect.String:
				fieldValue.SetString(string(val))
			case reflect.Slice:
				if field.fieldType.Elem().Kind() == reflect.Uint8 {
					fieldValue.SetBytes(val)
				} else {
					return fmt.Errorf("cannot assign string to field %s (type %v)", field.arrowName, field.fieldType)
				}
			}
		default:
			return fmt.Errorf("unsupported dictionary type %T", dict)
		}
	default:
		return fmt.Errorf("unsupported array type: %T for field %s", col, field.arrowName)
	}

	if field.fieldType == reflect.TypeOf(time.Time{}) {
		switch col.DataType().ID() {
		case arrow.TIMESTAMP:
			unit := col.DataType().(*arrow.TimestampType).Unit
			var ns int64
			switch arr := col.(type) {
			case *array.Timestamp:
				ns = int64(arr.Value(rowIdx))
			case *array.Int64:
				ns = arr.Value(rowIdx)
			default:
				return fmt.Errorf("unexpected array type for timestamp: %T", col)
			}
			var t time.Time
			switch unit {
			case arrow.Nanosecond:
				t = time.Unix(0, ns)
			case arrow.Microsecond:
				t = time.Unix(0, ns*1000)
			case arrow.Millisecond:
				t = time.Unix(0, ns*1000000)
			case arrow.Second:
				t = time.Unix(ns, 0)
			}
			fieldValue.Set(reflect.ValueOf(t))
		}
		return nil
	}

	return nil
}

// setMapValue sets a value in a map, handling type conversions.
// This, too, works directly with Arrow data.
func (d *Decoder) setMapValue(elem reflect.Value, fieldName string, col arrow.Array, rowIdx int) error {
	mapKey := reflect.ValueOf(fieldName)
	if col.IsNull(rowIdx) {
		// Don't set anything for null values in maps (acts like omitempty)
		return nil
	}

	mapValType := elem.Type().Elem()
	var val reflect.Value

	switch arr := col.(type) {
	case *array.Int64:
		if !reflect.TypeOf(arr.Value(rowIdx)).AssignableTo(mapValType) {
			return fmt.Errorf("cannot assign int64 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(arr.Value(rowIdx))
	case *array.Int8:
		value := int8(arr.Value(rowIdx))
		if !reflect.TypeOf(value).AssignableTo(mapValType) && !reflect.TypeOf(value).ConvertibleTo(mapValType) {
			return fmt.Errorf("cannot assign int8 to map value type %v", mapValType)
		}

		val = reflect.ValueOf(value)
		if val.Type().ConvertibleTo(mapValType) {
			val = val.Convert(mapValType)
		}
	case *array.Int16:
		value := int16(arr.Value(rowIdx))
		if !reflect.TypeOf(value).AssignableTo(mapValType) && !reflect.TypeOf(value).ConvertibleTo(mapValType) {
			return fmt.Errorf("cannot assign int16 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(value)
		if val.Type().ConvertibleTo(mapValType) {
			val = val.Convert(mapValType)
		}
	case *array.Int32:
		value := int32(arr.Value(rowIdx))
		if !reflect.TypeOf(value).AssignableTo(mapValType) && !reflect.TypeOf(value).ConvertibleTo(mapValType) {
			return fmt.Errorf("cannot assign int32 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(value)
		if val.Type().ConvertibleTo(mapValType) {
			val = val.Convert(mapValType)
		}
	case *array.Uint8:
		value := uint8(arr.Value(rowIdx))
		if !reflect.TypeOf(value).AssignableTo(mapValType) && !reflect.TypeOf(value).ConvertibleTo(mapValType) {
			return fmt.Errorf("cannot assign uint8 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(value)
		if val.Type().ConvertibleTo(mapValType) {
			val = val.Convert(mapValType)
		}
	case *array.Uint16:
		value := uint16(arr.Value(rowIdx))
		if !reflect.TypeOf(value).AssignableTo(mapValType) && !reflect.TypeOf(value).ConvertibleTo(mapValType) {
			return fmt.Errorf("cannot assign uint16 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(value)
		if val.Type().ConvertibleTo(mapValType) {
			val = val.Convert(mapValType)
		}
	case *array.Uint32:
		value := uint32(arr.Value(rowIdx))
		if !reflect.TypeOf(value).AssignableTo(mapValType) && !reflect.TypeOf(value).ConvertibleTo(mapValType) {
			return fmt.Errorf("cannot assign int32 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(value)
		if val.Type().ConvertibleTo(mapValType) {
			val = val.Convert(mapValType)
		}
	case *array.Uint64:
		if !reflect.TypeOf(arr.Value(rowIdx)).AssignableTo(mapValType) {
			return fmt.Errorf("cannot assign uint64 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(arr.Value(rowIdx))
	case *array.Float32:
		value := float32(arr.Value(rowIdx))
		if !reflect.TypeOf(value).AssignableTo(mapValType) && !reflect.TypeOf(value).ConvertibleTo(mapValType) {
			return fmt.Errorf("cannot assign float32 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(value)
		if val.Type().ConvertibleTo(mapValType) {
			val = val.Convert(mapValType)
		}
	case *array.Float64:
		if !reflect.TypeOf(arr.Value(rowIdx)).AssignableTo(mapValType) {
			return fmt.Errorf("cannot assign float64 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(arr.Value(rowIdx))
	case *array.String:
		val = reflect.ValueOf(arr.Value(rowIdx)) // Get string *directly*.
		if val.Type().Kind() == reflect.String && mapValType.Kind() == reflect.Slice {
			if mapValType.Elem().Kind() == reflect.Uint8 {
				val = reflect.ValueOf([]byte(val.String())) // Convert to []byte
			}
		}

	case *array.Boolean:
		if !reflect.TypeOf(arr.Value(rowIdx)).AssignableTo(mapValType) {
			return fmt.Errorf("cannot assign bool to map value type %v", mapValType)
		}
		val = reflect.ValueOf(arr.Value(rowIdx))
	case *array.Timestamp:
		if mapValType == reflect.TypeOf(time.Time{}) {
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
			val = reflect.ValueOf(t)
		} else {
			val = reflect.ValueOf(int64(arr.Value(rowIdx)))
		}
	case *array.Date32:
		if mapValType == reflect.TypeOf(time.Time{}) {
			days := int64(arr.Value(rowIdx))
			t := time.Unix(days*86400, 0)
			val = reflect.ValueOf(t)
		} else {
			value := int32(arr.Value(rowIdx))
			if !reflect.TypeOf(value).AssignableTo(mapValType) && !reflect.TypeOf(value).ConvertibleTo(mapValType) {
				return fmt.Errorf("cannot assign date32 to map value type %v", mapValType)
			}
			val = reflect.ValueOf(value)
			if val.Type().ConvertibleTo(mapValType) {
				val = val.Convert(mapValType)
			}
		}
	case *array.Date64:
		if mapValType == reflect.TypeOf(time.Time{}) {
			ms := int64(arr.Value(rowIdx))
			t := time.Unix(0, ms*1000000)
			val = reflect.ValueOf(t)
		} else {
			if !reflect.TypeOf(arr.Value(rowIdx)).AssignableTo(mapValType) {
				return fmt.Errorf("cannot assign date64 to map value type %v", mapValType)
			}
			val = reflect.ValueOf(int64(arr.Value(rowIdx)))
		}
	case *array.Time32:
		value := int32(arr.Value(rowIdx))
		if !reflect.TypeOf(value).AssignableTo(mapValType) && !reflect.TypeOf(value).ConvertibleTo(mapValType) {
			return fmt.Errorf("cannot assign date32 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(value)
		if val.Type().ConvertibleTo(mapValType) {
			val = val.Convert(mapValType)
		}
	case *array.Time64:
		if !reflect.TypeOf(arr.Value(rowIdx)).AssignableTo(mapValType) {
			return fmt.Errorf("cannot assign time64 to map value type %v", mapValType)
		}
		val = reflect.ValueOf(int64(arr.Value(rowIdx)))
	case *array.Binary:
		value := arr.Value(rowIdx)
		binaryVal := reflect.New(mapValType).Elem()
		switch mapValType.Kind() {
		case reflect.String:
			binaryVal.SetString(string(value))
			val = binaryVal
		case reflect.Slice:
			if mapValType.Elem().Kind() == reflect.Uint8 {
				binaryVal.SetBytes(value) // Direct bytes assignment.
				val = binaryVal           // Wrap in reflect.Value.
			} else {
				return fmt.Errorf("cannot assign binary to map value type %v", mapValType)
			}
		default:
			return fmt.Errorf("cannot assign binary to map value type %v", mapValType)

		}
	case *array.Dictionary:
		index := arr.GetValueIndex(rowIdx)
		switch dict := arr.Dictionary().(type) {
		case *array.String:
			val = reflect.ValueOf(dict.Value(index)) // Get string *directly*.
			if val.Type().Kind() == reflect.String && mapValType.Kind() == reflect.Slice {
				if mapValType.Elem().Kind() == reflect.Uint8 {
					val = reflect.ValueOf([]byte(val.String())) // Convert to []byte
				}
			}
		case *array.Binary:
			value := dict.Value(index)
			binaryVal := reflect.New(mapValType).Elem()
			switch mapValType.Kind() {
			case reflect.String:
				binaryVal.SetString(string(value))
				val = binaryVal
			case reflect.Slice:
				if mapValType.Elem().Kind() == reflect.Uint8 {
					binaryVal.SetBytes(value) // Direct bytes assignment.
					val = binaryVal
				} else {
					return fmt.Errorf("cannot assign binary to map value type %v", mapValType)
				}
			}

		default:
			return fmt.Errorf("unsupported dictionary type %T", dict)
		}
	default:
		return fmt.Errorf("unsupported array type %T for map field %s", col, fieldName)
	}

	if mapValType == reflect.TypeOf(time.Time{}) {
		switch col.DataType().ID() {
		case arrow.TIMESTAMP:
			unit := col.DataType().(*arrow.TimestampType).Unit
			var ns int64
			switch arr := col.(type) {
			case *array.Timestamp:
				ns = int64(arr.Value(rowIdx))
			case *array.Int64:
				ns = arr.Value(rowIdx)
			default:
				return fmt.Errorf("unexpected array type for timestamp: %T", col)
			}
			var t time.Time
			switch unit {
			case arrow.Nanosecond:
				t = time.Unix(0, ns)
			case arrow.Microsecond:
				t = time.Unix(0, ns*1000)
			case arrow.Millisecond:
				t = time.Unix(0, ns*1000000)
			case arrow.Second:
				t = time.Unix(ns, 0)
			}
			val = reflect.ValueOf(t)
		}
	}

	// Assign the value to the map
	elem.SetMapIndex(mapKey, val)
	return nil
}

// Helper functions for string operations.
func contains(s, substr string) bool {
	return index(s, substr) >= 0
}
func index(s, substr string) int {
	n := len(substr)
	switch {
	case n == 0:
		return 0
	case n == 1:
		// special case worth making fast
		c := substr[0]
		for i := 0; i < len(s); i++ {
			if s[i] == c {
				return i
			}
		}
		return -1
	case n == len(s):
		if substr == s {
			return 0
		}
		return -1
	case n > len(s):
		return -1
	}
	// Rabin-Karp search from Go strings package
	hashss, pow := hashStr(substr)
	sLen := len(s)
	var h uint32
	for i := 0; i < n; i++ {
		h = h*primeRK + uint32(s[i])
	}
	if h == hashss && s[:n] == substr {
		return 0
	}
	for i := n; i < sLen; {
		h *= primeRK
		h += uint32(s[i])
		h -= pow * uint32(s[i-n])
		i++
		if h == hashss && s[i-n:i] == substr {
			return i - n
		}
	}
	return -1
}

// primeRK is the prime base used in Rabin-Karp algorithm.
const primeRK = 16777619

// hashStr returns the hash and the appropriate multiplicative
// factor for use in Rabin-Karp algorithm.
func hashStr(sep string) (uint32, uint32) {
	hash := uint32(0)
	for i := 0; i < len(sep); i++ {
		hash = hash*primeRK + uint32(sep[i])
	}
	var pow, sq uint32 = 1, primeRK
	for i := len(sep); i > 0; i >>= 1 {
		if i&1 != 0 {
			pow *= sq
		}
		sq *= sq
	}
	return hash, pow
}

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
