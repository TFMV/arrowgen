package schema

import (
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// SchemaOptions configures how the schema is inferred.
type SchemaOptions struct {
	// EnableDictionary enables dictionary encoding for string fields.
	EnableDictionary bool
	// DictionaryIDGen is used to generate unique IDs for dictionary fields.
	DictionaryIDGen *uint64
	// MaxNestingDepth is the maximum allowed nesting depth for structs.
	MaxNestingDepth int
}

// DefaultSchemaOptions returns the default schema options.
func DefaultSchemaOptions() *SchemaOptions {
	var dictID uint64
	return &SchemaOptions{
		EnableDictionary: true,
		DictionaryIDGen:  &dictID,
		MaxNestingDepth:  10,
	}
}

// SchemaFromStruct infers an Arrow schema from a native Go struct.
// It uses the "arrow" struct tag (if available) to determine field names.
func SchemaFromStruct(s interface{}) (*arrow.Schema, error) {
	return SchemaFromStructWithOptions(s, DefaultSchemaOptions())
}

// SchemaFromStructWithOptions infers an Arrow schema with custom options.
func SchemaFromStructWithOptions(s interface{}, opts *SchemaOptions) (*arrow.Schema, error) {
	typ := reflect.TypeOf(s)
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("SchemaFromStruct expects a struct but got %T", s)
	}

	fields, err := inferStructFields(typ, opts, 0)
	if err != nil {
		return nil, err
	}

	return arrow.NewSchema(fields, nil), nil
}

// inferStructFields recursively infers Arrow fields from a struct type.
func inferStructFields(typ reflect.Type, opts *SchemaOptions, depth int) ([]arrow.Field, error) {
	if depth > opts.MaxNestingDepth {
		return nil, fmt.Errorf("maximum nesting depth exceeded (%d)", opts.MaxNestingDepth)
	}

	var fields []arrow.Field
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// Check for "arrow" tag override
		name := field.Tag.Get("arrow")
		if name == "" {
			name = field.Name
		}

		// Check for "-" to skip the field
		if name == "-" {
			continue
		}

		arrowType, err := goTypeToArrowType(field.Type, opts, depth+1)
		if err != nil {
			return nil, fmt.Errorf("field %s: %v", field.Name, err)
		}

		fields = append(fields, arrow.Field{Name: name, Type: arrowType, Nullable: true})
	}

	return fields, nil
}

// SchemaFromMap infers an Arrow schema from a map[string]interface{}.
// The sample map should contain representative data types.
func SchemaFromMap(m map[string]interface{}) (*arrow.Schema, error) {
	return SchemaFromMapWithOptions(m, DefaultSchemaOptions())
}

// SchemaFromMapWithOptions infers an Arrow schema from a map with custom options.
func SchemaFromMapWithOptions(m map[string]interface{}, opts *SchemaOptions) (*arrow.Schema, error) {
	var fields []arrow.Field
	for key, value := range m {
		arrowType, err := inferArrowType(reflect.TypeOf(value), opts, 0)
		if err != nil {
			return nil, fmt.Errorf("key %s: %v", key, err)
		}
		fields = append(fields, arrow.Field{Name: key, Type: arrowType, Nullable: true})
	}
	return arrow.NewSchema(fields, nil), nil
}

// goTypeToArrowType maps a Go type to an Arrow DataType.
func goTypeToArrowType(t reflect.Type, opts *SchemaOptions, depth int) (arrow.DataType, error) {
	// Handle pointer types by dereferencing
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return inferArrowType(t, opts, depth)
}

// inferArrowType converts common Go types to Arrow types.
func inferArrowType(t reflect.Type, opts *SchemaOptions, depth int) (arrow.DataType, error) {
	switch t.Kind() {
	case reflect.Int8:
		return arrow.PrimitiveTypes.Int8, nil
	case reflect.Int16:
		return arrow.PrimitiveTypes.Int16, nil
	case reflect.Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case reflect.Int64, reflect.Int:
		return arrow.PrimitiveTypes.Int64, nil
	case reflect.Uint8:
		return arrow.PrimitiveTypes.Uint8, nil
	case reflect.Uint16:
		return arrow.PrimitiveTypes.Uint16, nil
	case reflect.Uint32:
		return arrow.PrimitiveTypes.Uint32, nil
	case reflect.Uint64, reflect.Uint:
		return arrow.PrimitiveTypes.Uint64, nil
	case reflect.Float32:
		return arrow.PrimitiveTypes.Float32, nil
	case reflect.Float64:
		return arrow.PrimitiveTypes.Float64, nil
	case reflect.Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case reflect.String:
		if opts.EnableDictionary {
			// Create a dictionary type with string values
			return &arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int32,
				ValueType: arrow.BinaryTypes.String,
				Ordered:   true,
			}, nil
		}
		return arrow.BinaryTypes.String, nil
	case reflect.Struct:
		// Special-case time.Time
		if t == reflect.TypeOf(time.Time{}) {
			return arrow.FixedWidthTypes.Timestamp_ns, nil
		}

		// Handle nested structs
		fields, err := inferStructFields(t, opts, depth)
		if err != nil {
			return nil, err
		}
		return arrow.StructOf(fields...), nil

	case reflect.Slice, reflect.Array:
		// Handle arrays and slices
		elemType, err := goTypeToArrowType(t.Elem(), opts, depth+1)
		if err != nil {
			return nil, fmt.Errorf("slice/array element: %v", err)
		}
		return arrow.ListOf(elemType), nil

	case reflect.Map:
		// Handle maps
		keyType, err := goTypeToArrowType(t.Key(), opts, depth+1)
		if err != nil {
			return nil, fmt.Errorf("map key: %v", err)
		}
		valueType, err := goTypeToArrowType(t.Elem(), opts, depth+1)
		if err != nil {
			return nil, fmt.Errorf("map value: %v", err)
		}
		return arrow.MapOf(keyType, valueType), nil

	default:
		return nil, fmt.Errorf("unsupported Go type: %v", t)
	}
}
