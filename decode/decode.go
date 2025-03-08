package decode

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/TFMV/arrowgen/internal/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Decoder decodes an Arrow Record into native Go types (structs or maps).
type Decoder struct {
	schema    *arrow.Schema
	memPool   *pool.MemPool
	valuePool *pool.ValuePool
	simdPool  *pool.SIMDPool
	fieldMap  sync.Map // Thread-safe map for field indices
}

// NewDecoder creates a new Decoder for the provided Arrow schema.
func NewDecoder(schema *arrow.Schema) *Decoder {
	return &Decoder{
		schema:    schema,
		memPool:   pool.NewMemPool(),
		valuePool: pool.NewValuePool(),
		simdPool:  pool.NewSIMDPool(),
	}
}

// Decode decodes an Arrow Record into the provided output pointer.
func (d *Decoder) Decode(record arrow.Record, out interface{}) error {
	outVal := reflect.ValueOf(out)
	if outVal.Kind() != reflect.Ptr || outVal.IsNil() {
		return fmt.Errorf("out must be a non-nil pointer")
	}

	elemType := outVal.Elem().Type()
	sliceVal := outVal.Elem()

	// Initialize or resize the output slice
	numRows := int(record.NumRows())
	if sliceVal.Cap() < numRows {
		sliceVal.Set(reflect.MakeSlice(sliceVal.Type(), numRows, numRows))
	} else {
		sliceVal.SetLen(numRows)
	}

	// Pre-initialize map elements if needed
	if elemType.Elem().Kind() == reflect.Map {
		for i := 0; i < numRows; i++ {
			elem := sliceVal.Index(i)
			if elem.IsNil() {
				elem.Set(reflect.MakeMap(elemType.Elem()))
			}
		}
	}

	// Determine optimal number of goroutines based on data size
	numCols := int(record.NumCols())
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

	// Limit goroutines to number of columns
	if numGoroutines > numCols {
		numGoroutines = numCols
	}

	// Create work channel and error channel
	workChan := make(chan int, numCols)
	errChan := make(chan error, numCols)
	var wg sync.WaitGroup

	// Create a slice of mutexes for each row
	rowMutexes := make([]sync.Mutex, numRows)

	// Start worker goroutines
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
				values := d.valuePool.Get()
				if cap(values) < col.Len() {
					values = make([]interface{}, col.Len())
				} else {
					values = values[:col.Len()]
				}

				if err := d.extractValues(col, values); err != nil {
					errChan <- fmt.Errorf("failed to extract values from column %s: %w", fieldName, err)
					return
				}

				// Set values for each row
				for rowIdx := 0; rowIdx < numRows; rowIdx++ {
					elem := sliceVal.Index(rowIdx)
					if elem.Kind() == reflect.Map {
						if values[rowIdx] != nil {
							rowMutexes[rowIdx].Lock()
							elem.SetMapIndex(reflect.ValueOf(fieldName), reflect.ValueOf(values[rowIdx]))
							rowMutexes[rowIdx].Unlock()
						}
					} else {
						if err := d.setStructField(elem, fieldName, values[rowIdx]); err != nil {
							errChan <- fmt.Errorf("failed to set field %s: %w", fieldName, err)
							return
						}
					}
				}

				// Return values slice to pool
				d.valuePool.Put(values)
			}
		}()
	}

	// Send work to goroutines
	for i := 0; i < numCols; i++ {
		workChan <- i
	}
	close(workChan)

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// extractValues extracts values from an Arrow array into a slice
func (d *Decoder) extractValues(col arrow.Array, values []interface{}) error {
	if col == nil {
		return fmt.Errorf("column is nil")
	}

	// Extract values based on array type
	switch arr := col.(type) {
	case *array.Dictionary:
		// Get the dictionary values
		dict := arr.Dictionary()
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				// Get the index and look up the value in the dictionary
				idx := arr.GetValueIndex(i)
				switch dict := dict.(type) {
				case *array.String:
					values[i] = dict.Value(int(idx))
				case *array.Binary:
					values[i] = string(dict.Value(int(idx)))
				default:
					return fmt.Errorf("unsupported dictionary value type: %T", dict)
				}
			}
		}
	case *array.Int64:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractInt64ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Int8:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractInt8ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Int16:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractInt16ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Int32:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractInt32ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint8:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractUint8ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint16:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractUint16ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint32:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractUint32ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint64:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractUint64ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Float32:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractFloat32ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Float64:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractFloat64ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.String:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractStringValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Boolean:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractBooleanValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Timestamp:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractTimestampValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = int64(arr.Value(i))
			}
		}
	case *array.Date32:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractDate32ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = int32(arr.Value(i))
			}
		}
	case *array.Date64:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractDate64ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = int64(arr.Value(i))
			}
		}
	case *array.Time32:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractTime32ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = int32(arr.Value(i))
			}
		}
	case *array.Time64:
		// Use SIMD-style processing for better performance
		if len(values) >= 8 {
			return d.extractTime64ValuesSIMD(arr, values)
		}

		// Regular processing for small slices
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = int64(arr.Value(i))
			}
		}
	case *array.Binary:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.FixedSizeBinary:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.List:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				// Extract the list values
				start := arr.Offsets()[i]
				end := arr.Offsets()[i+1]
				length := int(end - start)

				listValues := d.valuePool.Get()
				listValues = listValues[:length]

				// Get the list array
				listArray := arr.ListValues()

				// Extract values from the list array
				if err := d.extractValues(array.NewSlice(listArray, int64(start), int64(end)), listValues); err != nil {
					return err
				}

				// Store the list values
				values[i] = listValues
			}
		}
	case *array.FixedSizeList:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				// Extract the list values
				listSize := int(arr.Len())
				listValues := d.valuePool.Get()
				listValues = listValues[:listSize]

				// Get the list array
				listArray := arr.ListValues()

				// Extract values from the list array
				start := int64(i * listSize)
				end := int64((i + 1) * listSize)
				if err := d.extractValues(array.NewSlice(listArray, start, end), listValues); err != nil {
					return err
				}

				// Store the list values
				values[i] = listValues
			}
		}
	case *array.Struct:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				// Create a map for the struct fields
				structMap := make(map[string]interface{})

				// Extract values for each field
				for j := 0; j < arr.NumField(); j++ {
					// Get the field name from the struct type
					structType, ok := arr.DataType().(*arrow.StructType)
					if !ok {
						return fmt.Errorf("expected struct type, got %T", arr.DataType())
					}
					fieldName := structType.Field(j).Name

					fieldArray := arr.Field(j)

					// Extract the field value
					fieldValues := d.valuePool.Get()
					fieldValues = fieldValues[:1]

					// Extract values from the field array
					if err := d.extractValues(array.NewSlice(fieldArray, int64(i), int64(i+1)), fieldValues); err != nil {
						return err
					}

					// Store the field value
					structMap[fieldName] = fieldValues[0]

					// Return the field values to the pool
					d.valuePool.Put(fieldValues)
				}

				// Store the struct map
				values[i] = structMap
			}
		}
	case *array.Map:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				// Create a map for the map entries
				mapEntries := make(map[interface{}]interface{})

				// Get the map offset and length
				start := arr.Offsets()[i]
				end := arr.Offsets()[i+1]
				length := int(end - start)

				// Get the key and item arrays from the struct array
				entries := arr.Items()
				structArr, ok := entries.(*array.Struct)
				if !ok {
					return fmt.Errorf("expected struct array for map items, got %T", entries)
				}

				keyArray := structArr.Field(0)
				itemArray := structArr.Field(1)

				// Extract keys and values
				keys := d.valuePool.Get()
				keys = keys[:length]
				items := d.valuePool.Get()
				items = items[:length]

				// Extract values from the key and item arrays
				if err := d.extractValues(array.NewSlice(keyArray, int64(start), int64(end)), keys); err != nil {
					return err
				}
				if err := d.extractValues(array.NewSlice(itemArray, int64(start), int64(end)), items); err != nil {
					return err
				}

				// Build the map
				for j := 0; j < length; j++ {
					mapEntries[keys[j]] = items[j]
				}

				// Store the map
				values[i] = mapEntries

				// Return the keys and items to the pool
				d.valuePool.Put(keys)
				d.valuePool.Put(items)
			}
		}
	case *array.Decimal128:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Decimal256:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	default:
		return fmt.Errorf("unsupported array type: %T", col)
	}
	return nil
}

// extractInt64ValuesSIMD extracts int64 values using SIMD-style processing
func (d *Decoder) extractInt64ValuesSIMD(arr *array.Int64, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractInt8ValuesSIMD extracts int8 values using SIMD-style processing
func (d *Decoder) extractInt8ValuesSIMD(arr *array.Int8, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractInt16ValuesSIMD extracts int16 values using SIMD-style processing
func (d *Decoder) extractInt16ValuesSIMD(arr *array.Int16, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractInt32ValuesSIMD extracts int32 values using SIMD-style processing
func (d *Decoder) extractInt32ValuesSIMD(arr *array.Int32, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractUint8ValuesSIMD extracts uint8 values using SIMD-style processing
func (d *Decoder) extractUint8ValuesSIMD(arr *array.Uint8, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractUint16ValuesSIMD extracts uint16 values using SIMD-style processing
func (d *Decoder) extractUint16ValuesSIMD(arr *array.Uint16, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractUint32ValuesSIMD extracts uint32 values using SIMD-style processing
func (d *Decoder) extractUint32ValuesSIMD(arr *array.Uint32, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractUint64ValuesSIMD extracts uint64 values using SIMD-style processing
func (d *Decoder) extractUint64ValuesSIMD(arr *array.Uint64, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractFloat32ValuesSIMD extracts float32 values using SIMD-style processing
func (d *Decoder) extractFloat32ValuesSIMD(arr *array.Float32, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractFloat64ValuesSIMD extracts float64 values using SIMD-style processing
func (d *Decoder) extractFloat64ValuesSIMD(arr *array.Float64, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractStringValuesSIMD extracts string values using SIMD-style processing
func (d *Decoder) extractStringValuesSIMD(arr *array.String, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractBooleanValuesSIMD extracts boolean values using SIMD-style processing
func (d *Decoder) extractBooleanValuesSIMD(arr *array.Boolean, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractTimestampValuesSIMD extracts timestamp values using SIMD-style processing
func (d *Decoder) extractTimestampValuesSIMD(arr *array.Timestamp, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractDate32ValuesSIMD extracts date32 values using SIMD-style processing
func (d *Decoder) extractDate32ValuesSIMD(arr *array.Date32, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractDate64ValuesSIMD extracts date64 values using SIMD-style processing
func (d *Decoder) extractDate64ValuesSIMD(arr *array.Date64, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractTime32ValuesSIMD extracts time32 values using SIMD-style processing
func (d *Decoder) extractTime32ValuesSIMD(arr *array.Time32, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// extractTime64ValuesSIMD extracts time64 values using SIMD-style processing
func (d *Decoder) extractTime64ValuesSIMD(arr *array.Time64, values []interface{}) error {
	// Get a slice from the pool to store nulls
	nulls := d.simdPool.GetBoolSlice(len(values))
	defer d.simdPool.PutBoolSlice(nulls)

	// Process nulls in chunks of 8 for better cache utilization
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			nulls[i+j] = arr.IsNull(i + j)
		}
	}

	// Handle remaining nulls
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		nulls[i] = arr.IsNull(i)
	}

	// Process values in chunks of 8
	for i := 0; i <= len(values)-8; i += 8 {
		for j := 0; j < 8; j++ {
			if !nulls[i+j] {
				values[i+j] = arr.Value(i + j)
			} else {
				values[i+j] = nil
			}
		}
	}

	// Handle remaining values
	for i := (len(values) / 8) * 8; i < len(values); i++ {
		if !nulls[i] {
			values[i] = arr.Value(i)
		} else {
			values[i] = nil
		}
	}

	return nil
}

// setStructField sets a field on a struct using cached field indices
func (d *Decoder) setStructField(elem reflect.Value, fieldName string, value interface{}) error {
	// Try to find field by arrow tag first
	t := elem.Type()
	var field reflect.Value
	var found bool

	// Check cache first
	if fieldIdxI, ok := d.fieldMap.Load(fieldName); ok {
		field = elem.Field(fieldIdxI.(int))
		found = true
	} else {
		// Cache miss - look up field and cache it
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name := f.Tag.Get("arrow")
			if name == "" {
				name = f.Name
			}
			if name == fieldName {
				d.fieldMap.Store(fieldName, i)
				field = elem.Field(i)
				found = true
				break
			}
		}
	}

	if !found {
		return fmt.Errorf("field %s not found", fieldName)
	}

	// Set the value
	if value == nil {
		field.Set(reflect.Zero(field.Type()))
		return nil
	}

	val := reflect.ValueOf(value)
	if !val.Type().AssignableTo(field.Type()) {
		// Handle special cases
		switch {
		case field.Type() == reflect.TypeOf(time.Time{}):
			switch val.Kind() {
			case reflect.Int64:
				field.Set(reflect.ValueOf(time.Unix(0, val.Int())))
				return nil
			}
		}

		if val.Type().ConvertibleTo(field.Type()) {
			val = val.Convert(field.Type())
		} else {
			return fmt.Errorf("cannot assign %v (type %v) to field %s (type %v)", value, val.Type(), fieldName, field.Type())
		}
	}

	field.Set(val)
	return nil
}
