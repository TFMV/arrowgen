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
	fieldMap  sync.Map // Thread-safe map for field indices
}

// NewDecoder creates a new Decoder for the provided Arrow schema.
func NewDecoder(schema *arrow.Schema) *Decoder {
	return &Decoder{
		schema:    schema,
		memPool:   pool.NewMemPool(),
		valuePool: pool.NewValuePool(),
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
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Int8:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Int16:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Int32:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint8:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint16:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint32:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint64:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Float32:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Float64:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.String:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Boolean:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = arr.Value(i)
			}
		}
	case *array.Timestamp:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = int64(arr.Value(i))
			}
		}
	case *array.Date32:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = int32(arr.Value(i))
			}
		}
	case *array.Date64:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = int64(arr.Value(i))
			}
		}
	case *array.Time32:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = int32(arr.Value(i))
			}
		}
	case *array.Time64:
		for i := 0; i < len(values); i++ {
			if arr.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = int64(arr.Value(i))
			}
		}
	default:
		return fmt.Errorf("unsupported array type: %T", col)
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
