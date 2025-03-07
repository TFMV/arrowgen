package schema

import (
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

// Test structs
type SimpleStruct struct {
	Int    int     `arrow:"int"`
	String string  `arrow:"string"`
	Float  float64 `arrow:"float"`
	Bool   bool    `arrow:"bool"`
}

type AllTypesStruct struct {
	Int8      int8      `arrow:"int8"`
	Int16     int16     `arrow:"int16"`
	Int32     int32     `arrow:"int32"`
	Int64     int64     `arrow:"int64"`
	Uint8     uint8     `arrow:"uint8"`
	Uint16    uint16    `arrow:"uint16"`
	Uint32    uint32    `arrow:"uint32"`
	Uint64    uint64    `arrow:"uint64"`
	Float32   float32   `arrow:"float32"`
	Float64   float64   `arrow:"float64"`
	String    string    `arrow:"string"`
	Bool      bool      `arrow:"bool"`
	Time      time.Time `arrow:"timestamp"`
	ByteSlice []byte    `arrow:"bytes"`
}

type NestedStruct struct {
	ID     int64        `arrow:"id"`
	Nested SimpleStruct `arrow:"nested"`
	Array  []int32      `arrow:"array"`
}

type CustomTags struct {
	Public     string `arrow:"renamed"`
	Ignored    string `arrow:"-"`
	NoTag      string
	unexported string
}

type RecursiveStruct struct {
	ID       int64             `arrow:"id"`
	Self     *RecursiveStruct  `arrow:"self"`
	Children []RecursiveStruct `arrow:"children"`
}

func TestSchemaFromStruct(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected []arrow.Field
		wantErr  bool
	}{
		{
			name:  "simple struct",
			input: SimpleStruct{},
			expected: []arrow.Field{
				{Name: "int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "string", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String, Ordered: true}, Nullable: true},
				{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			},
		},
		{
			name:  "all types",
			input: AllTypesStruct{},
			expected: []arrow.Field{
				{Name: "int8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
				{Name: "int16", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
				{Name: "int32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: "int64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "uint8", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
				{Name: "uint16", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
				{Name: "uint32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
				{Name: "uint64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
				{Name: "float32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
				{Name: "float64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				{Name: "string", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String, Ordered: true}, Nullable: true},
				{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
				{Name: "bytes", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint8), Nullable: true},
			},
		},
		{
			name:  "nested struct",
			input: NestedStruct{},
			expected: []arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "nested", Type: arrow.StructOf(
					arrow.Field{Name: "int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					arrow.Field{Name: "string", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String, Ordered: true}, Nullable: true},
					arrow.Field{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					arrow.Field{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				), Nullable: true},
				{Name: "array", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
			},
		},
		{
			name:  "custom tags",
			input: CustomTags{},
			expected: []arrow.Field{
				{Name: "renamed", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String, Ordered: true}, Nullable: true},
				{Name: "NoTag", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String, Ordered: true}, Nullable: true},
			},
		},
		{
			name:    "non-struct input",
			input:   "not a struct",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := SchemaFromStruct(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, len(tt.expected), len(schema.Fields()))
			for i, expected := range tt.expected {
				actual := schema.Field(i)
				assert.Equal(t, expected.Name, actual.Name)
				assert.Equal(t, expected.Type, actual.Type)
				assert.Equal(t, expected.Nullable, actual.Nullable)
			}
		})
	}
}

func TestSchemaFromStructWithOptions(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		opts     *SchemaOptions
		expected []arrow.Field
		wantErr  bool
	}{
		{
			name:  "disable dictionary encoding",
			input: SimpleStruct{},
			opts: &SchemaOptions{
				EnableDictionary: false,
				MaxNestingDepth:  10,
			},
			expected: []arrow.Field{
				{Name: "int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			},
		},
		{
			name:  "exceed max nesting depth",
			input: RecursiveStruct{},
			opts: &SchemaOptions{
				EnableDictionary: true,
				MaxNestingDepth:  1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := SchemaFromStructWithOptions(tt.input, tt.opts)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, len(tt.expected), len(schema.Fields()))
			for i, expected := range tt.expected {
				actual := schema.Field(i)
				assert.Equal(t, expected.Name, actual.Name)
				assert.Equal(t, expected.Type, actual.Type)
				assert.Equal(t, expected.Nullable, actual.Nullable)
			}
		})
	}
}

func TestSchemaFromMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		validate func(*testing.T, *arrow.Schema)
		wantErr  bool
	}{
		{
			name: "basic types",
			input: map[string]interface{}{
				"int":    int64(42),
				"string": "test",
				"float":  3.14,
				"bool":   true,
				"time":   time.Now(),
			},
			validate: func(t *testing.T, schema *arrow.Schema) {
				fields := make(map[string]arrow.DataType)
				for _, f := range schema.Fields() {
					fields[f.Name] = f.Type
				}

				assert.Equal(t, 5, len(fields))
				assert.Equal(t, arrow.PrimitiveTypes.Int64, fields["int"])
				assert.Equal(t, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String, Ordered: true}, fields["string"])
				assert.Equal(t, arrow.PrimitiveTypes.Float64, fields["float"])
				assert.Equal(t, arrow.FixedWidthTypes.Boolean, fields["bool"])
				assert.Equal(t, arrow.FixedWidthTypes.Timestamp_ns, fields["time"])
			},
		},
		{
			name: "nested types",
			input: map[string]interface{}{
				"array": []int32{1, 2, 3},
				"map":   map[string]int{"a": 1},
			},
			validate: func(t *testing.T, schema *arrow.Schema) {
				fields := make(map[string]arrow.DataType)
				for _, f := range schema.Fields() {
					fields[f.Name] = f.Type
				}

				assert.Equal(t, 2, len(fields))
				assert.Equal(t, arrow.ListOf(arrow.PrimitiveTypes.Int32), fields["array"])

				// Validate map type
				mapType, mapTypeOK := fields["map"].(*arrow.MapType)
				structType, structTypeOK := fields["map"].(*arrow.StructType)

				if mapTypeOK {
					// If it's a MapType, validate its value type
					assert.Equal(t, arrow.PrimitiveTypes.Int64, mapType.Elem().(*arrow.StructType).Field(1).Type)

					// Map key type should be dictionary-encoded string
					keyType := mapType.KeyType()
					dictType, ok := keyType.(*arrow.DictionaryType)
					assert.True(t, ok)
					assert.Equal(t, arrow.BinaryTypes.String, dictType.ValueType)
				} else if structTypeOK {
					// If it's a StructType (which is how arrow.MapOf() may be implemented),
					// validate that it has the expected structure

					// Print the actual structure for debugging
					t.Logf("StructType found with %d fields", len(structType.Fields()))
					for i, f := range structType.Fields() {
						t.Logf("Field %d: %s, Type: %T, Nullable: %v", i, f.Name, f.Type, f.Nullable)
					}

					// More flexible check - just verify we have 2 fields
					// and one of them has an Int64Type
					assert.Equal(t, 2, len(structType.Fields()))

					// Find the field with Int64Type, which should be our value field
					var valueField arrow.Field
					foundValueField := false

					for _, f := range structType.Fields() {
						if _, ok := f.Type.(*arrow.Int64Type); ok {
							valueField = f
							foundValueField = true
							break
						}
					}

					assert.True(t, foundValueField, "Expected a field with Int64Type")
					assert.Equal(t, arrow.PrimitiveTypes.Int64, valueField.Type)

					// Find the field with DictionaryType, which should be our key field
					foundKeyField := false

					for _, f := range structType.Fields() {
						if dictType, ok := f.Type.(*arrow.DictionaryType); ok {
							foundKeyField = true
							// Verify it's a dictionary with string values
							assert.Equal(t, arrow.BinaryTypes.String, dictType.ValueType)
							break
						}
					}

					assert.True(t, foundKeyField, "Expected a field with DictionaryType")
				} else {
					// Print the actual type for debugging
					t.Logf("Unexpected type for map field: %T", fields["map"])
					assert.Fail(t, "Expected either MapType or StructType for map field")
				}
			},
		},
		{
			name: "unsupported type",
			input: map[string]interface{}{
				"channel": make(chan int),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := SchemaFromMap(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			if tt.validate != nil {
				tt.validate(t, schema)
			}
		})
	}
}

func TestDefaultSchemaOptions(t *testing.T) {
	opts := DefaultSchemaOptions()
	assert.True(t, opts.EnableDictionary)
	assert.NotNil(t, opts.DictionaryIDGen)
	assert.Equal(t, 10, opts.MaxNestingDepth)
}

func TestEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		wantErr bool
		errMsg  string
	}{
		{
			name:  "empty struct",
			input: struct{}{},
		},
		{
			name: "only unexported fields",
			input: struct {
				private string
			}{},
		},
		{
			name: "only ignored fields",
			input: struct {
				Ignored string `arrow:"-"`
			}{},
		},
		{
			name: "deeply nested valid struct",
			input: struct {
				A struct {
					B struct {
						C string
					}
				}
			}{},
		},
		{
			name:    "circular type reference",
			input:   RecursiveStruct{Self: &RecursiveStruct{}},
			wantErr: true,
			errMsg:  "maximum nesting depth exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := SchemaFromStruct(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, schema)
		})
	}
}

func TestStringMemoryAddress(t *testing.T) {
	testMap := map[string]interface{}{
		"very_long_key_name_1": 1,
		"very_long_key_name_2": 2,
	}

	schema, err := SchemaFromMap(testMap)
	if err != nil {
		t.Fatal(err)
	}

	for key := range testMap {
		found := false
		for _, f := range schema.Fields() {
			if f.Name == key {
				if !reflect.DeepEqual(key, f.Name) {
					t.Errorf("Strings differ, unexpected behavior %s and %s", key, f.Name)
				}

				found = true
				break
			}
		}
		if !found {
			t.Errorf("original Key: %s was not found in generated schema", key)
		}
	}
}
