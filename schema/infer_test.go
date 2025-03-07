package schema

import (
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

type Address struct {
	Street  string `arrow:"street"`
	City    string `arrow:"city"`
	Country string `arrow:"country"`
}

type Contact struct {
	Email string `arrow:"email"`
	Phone string `arrow:"phone"`
}

type NestedUser struct {
	ID        int64             `arrow:"id"`
	Name      string            `arrow:"name"`
	Address   Address           `arrow:"address"`
	Contact   Contact           `arrow:"contact"`
	Tags      []string          `arrow:"tags"`
	Metadata  map[string]string `arrow:"metadata"`
	CreatedAt time.Time         `arrow:"created_at"`
}

type User struct {
	ID       int64     `arrow:"id"`
	Email    string    `arrow:"email"`
	JoinedAt time.Time `arrow:"joined_at"`
}

func TestSchemaFromStruct(t *testing.T) {
	schema, err := SchemaFromStruct(User{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "email", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String,
			Ordered:   true,
		}, Nullable: true},
		{Name: "joined_at", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
	}

	if len(schema.Fields()) != len(expectedFields) {
		t.Fatalf("expected %d fields but got %d", len(expectedFields), len(schema.Fields()))
	}

	for i, f := range schema.Fields() {
		if f.Name != expectedFields[i].Name || !reflect.DeepEqual(f.Type, expectedFields[i].Type) {
			t.Errorf("expected field %v but got %v", expectedFields[i], f)
		}
	}
}

func TestNestedStructSchema(t *testing.T) {
	// Use non-dictionary encoding for this test
	opts := DefaultSchemaOptions()
	opts.EnableDictionary = false
	schema, err := SchemaFromStructWithOptions(NestedUser{}, opts)
	assert.NoError(t, err)

	// Verify top-level fields
	fields := schema.Fields()
	assert.Equal(t, 7, len(fields))
	assert.Equal(t, "id", fields[0].Name)
	assert.Equal(t, "name", fields[1].Name)
	assert.Equal(t, "address", fields[2].Name)
	assert.Equal(t, "contact", fields[3].Name)
	assert.Equal(t, "tags", fields[4].Name)
	assert.Equal(t, "metadata", fields[5].Name)
	assert.Equal(t, "created_at", fields[6].Name)

	// Verify address struct type
	addressType, ok := fields[2].Type.(*arrow.StructType)
	assert.True(t, ok)
	assert.Equal(t, 3, len(addressType.Fields()))
	assert.Equal(t, "street", addressType.Field(0).Name)
	assert.Equal(t, "city", addressType.Field(1).Name)
	assert.Equal(t, "country", addressType.Field(2).Name)

	// Verify contact struct type
	contactType, ok := fields[3].Type.(*arrow.StructType)
	assert.True(t, ok)
	assert.Equal(t, 2, len(contactType.Fields()))
	assert.Equal(t, "email", contactType.Field(0).Name)
	assert.Equal(t, "phone", contactType.Field(1).Name)

	// Verify tags list type
	tagsType, ok := fields[4].Type.(*arrow.ListType)
	assert.True(t, ok)
	assert.Equal(t, arrow.BinaryTypes.String, tagsType.Elem())

	// Verify metadata map type
	metadataType, ok := fields[5].Type.(*arrow.MapType)
	assert.True(t, ok)
	assert.True(t, arrow.TypeEqual(arrow.BinaryTypes.String, metadataType.KeyType()))
	assert.True(t, arrow.TypeEqual(arrow.BinaryTypes.String, metadataType.ItemType()))
}

func TestDictionaryEncoding(t *testing.T) {
	opts := DefaultSchemaOptions()
	opts.EnableDictionary = true

	schema, err := SchemaFromStructWithOptions(User{}, opts)
	assert.NoError(t, err)

	// Verify that the email field uses dictionary encoding
	fields := schema.Fields()
	assert.Equal(t, 3, len(fields))

	emailType, ok := fields[1].Type.(*arrow.DictionaryType)
	assert.True(t, ok)
	assert.Equal(t, arrow.PrimitiveTypes.Int32, emailType.IndexType)
	assert.Equal(t, arrow.BinaryTypes.String, emailType.ValueType)
	assert.True(t, emailType.Ordered)

	// Test with dictionary encoding disabled
	opts.EnableDictionary = false
	schema, err = SchemaFromStructWithOptions(User{}, opts)
	assert.NoError(t, err)

	// Verify that the email field is a regular string
	fields = schema.Fields()
	assert.Equal(t, arrow.BinaryTypes.String, fields[1].Type)
}

func TestMaxNestingDepth(t *testing.T) {
	type RecursiveStruct struct {
		Name     string           `arrow:"name"`
		Children *RecursiveStruct `arrow:"children"`
	}

	opts := DefaultSchemaOptions()
	opts.MaxNestingDepth = 2

	_, err := SchemaFromStructWithOptions(RecursiveStruct{}, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "maximum nesting depth exceeded (2)")
}

func TestSkipUnexportedFields(t *testing.T) {
	type StructWithUnexported struct {
		Name    string `arrow:"name"`
		private string
		Age     int `arrow:"age"`
	}

	schema, err := SchemaFromStruct(StructWithUnexported{})
	assert.NoError(t, err)

	fields := schema.Fields()
	assert.Equal(t, 2, len(fields))
	assert.Equal(t, "name", fields[0].Name)
	assert.Equal(t, "age", fields[1].Name)
}

func TestSkipTaggedFields(t *testing.T) {
	type StructWithSkipped struct {
		Name   string `arrow:"name"`
		Secret string `arrow:"-"`
		Age    int    `arrow:"age"`
	}

	schema, err := SchemaFromStruct(StructWithSkipped{})
	assert.NoError(t, err)

	fields := schema.Fields()
	assert.Equal(t, 2, len(fields))
	assert.Equal(t, "name", fields[0].Name)
	assert.Equal(t, "age", fields[1].Name)
}
