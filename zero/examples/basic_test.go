package examples

import (
	"fmt"
	"testing"

	"github.com/TFMV/arrowgen/zero/decode"
	"github.com/TFMV/arrowgen/zero/encode"
	"github.com/TFMV/arrowgen/zero/schema"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
)

func TestBasicUsage(t *testing.T) {
	// Create sample data
	users := []User{
		{ID: 1, Name: "Alice", Email: "alice@example.com", IsActive: true},
		{ID: 2, Name: "Bob", Email: "bob@example.com", IsActive: false},
		{ID: 3, Name: "Charlie", Email: "charlie@example.com", IsActive: true},
	}

	// Infer schema from struct
	schema, err := schema.SchemaFromStruct(User{})
	assert.NoError(t, err)
	assert.Equal(t, 4, len(schema.Fields()))

	// Print schema fields
	fmt.Println("Schema fields:")
	for i, field := range schema.Fields() {
		fmt.Printf("  Field %d: %s (Type: %s)\n", i+1, field.Name, field.Type)
	}

	// Create encoder with default settings (zero-allocation mode)
	encoder := encode.NewEncoder(schema)

	// Encode data to Arrow record
	record, err := encoder.Encode(users)
	assert.NoError(t, err)
	defer record.Release()

	assert.Equal(t, int64(3), record.NumRows())
	assert.Equal(t, int64(4), record.NumCols())

	// Print record columns
	fmt.Println("Record columns:")
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		fmt.Printf("  Column %d: %s (Type: %T)\n", i+1, record.Schema().Field(i).Name, col)

		// Print column values
		fmt.Printf("    Values: ")
		for j := 0; j < int(record.NumRows()); j++ {
			if col.IsNull(j) {
				fmt.Printf("null ")
			} else {
				switch c := col.(type) {
				case *array.Int64:
					fmt.Printf("%d ", c.Value(j))
				case *array.Dictionary:
					if c.IsNull(j) {
						fmt.Printf("null ")
					} else {
						index := c.GetValueIndex(j)
						dict := c.Dictionary().(*array.String)
						fmt.Printf("%s ", dict.Value(index))
					}
				case *array.String:
					fmt.Printf("%s ", c.Value(j))
				case *array.Boolean:
					fmt.Printf("%v ", c.Value(j))
				default:
					fmt.Printf("? ")
				}
			}
		}
		fmt.Println()
	}

	// Create decoder with default settings
	decoder := decode.NewDecoder(schema)

	// Decode Arrow record back to Go struct
	var decodedUsers []User
	err = decoder.Decode(record, &decodedUsers)
	assert.NoError(t, err)

	// Verify the results
	assert.Equal(t, 3, len(decodedUsers))

	// Print decoded users
	fmt.Println("Decoded users:")
	for i, user := range decodedUsers {
		fmt.Printf("  User %d: ID=%d, Name=%s, Email=%s, IsActive=%v\n",
			i+1, user.ID, user.Name, user.Email, user.IsActive)
	}

	// Check each user
	for i, user := range users {
		assert.Equal(t, user.ID, decodedUsers[i].ID)
		assert.Equal(t, user.Name, decodedUsers[i].Name)
		assert.Equal(t, user.Email, decodedUsers[i].Email)
		assert.Equal(t, user.IsActive, decodedUsers[i].IsActive)
	}
}
