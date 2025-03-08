package encode_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"

	"github.com/TFMV/arrowgen/zero/encode"
)

// User struct for testing
type User struct {
	ID       int64
	Name     string
	Email    string
	IsActive bool
	JoinedAt time.Time
}

func TestBasicUsage(t *testing.T) {
	// Define an Arrow schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "email", Type: arrow.BinaryTypes.String},
			{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "joined_at", Type: arrow.FixedWidthTypes.Timestamp_ns},
		},
		nil,
	)

	// Create an encoder with the schema
	encoder := encode.NewEncoder(schema, encode.WithMode(encode.ModeHighThroughput))

	joinedAt := time.Date(2023, 10, 26, 10, 0, 0, 0, time.UTC) // Example time

	// Create some sample data
	users := []User{
		{ID: 1, Name: "Alice", Email: "alice@example.com", IsActive: true, JoinedAt: joinedAt},
		{ID: 2, Name: "Bob", Email: "bob@example.com", JoinedAt: joinedAt.Add(time.Hour * 24)},
		{ID: 3, Name: "Charlie", Email: "charlie@example.com", IsActive: true, JoinedAt: joinedAt.Add(time.Hour * 48)},
	}

	// Encode the data into an Arrow record
	record, err := encoder.Encode(users)

	//Check for errors.
	assert.NoError(t, err)

	// Verify the number of rows and columns
	assert.Equal(t, int64(len(users)), record.NumRows())
	assert.Equal(t, int64(len(schema.Fields())), record.NumCols())

	// Access columns and data
	idColumn := record.Column(0).(*array.Int64)

	// Handle both string and dictionary-encoded string fields
	var nameStr, emailStr string
	switch nameCol := record.Column(1).(type) {
	case *array.Dictionary:
		nameIndex := nameCol.GetValueIndex(0)
		nameStr = nameCol.Dictionary().(*array.String).Value(nameIndex)
	case *array.String:
		nameStr = nameCol.Value(0)
	default:
		t.Fatalf("Unexpected type for name column: %T", record.Column(1))
	}

	switch emailCol := record.Column(2).(type) {
	case *array.Dictionary:
		emailIndex := emailCol.GetValueIndex(0)
		emailStr = emailCol.Dictionary().(*array.String).Value(emailIndex)
	case *array.String:
		emailStr = emailCol.Value(0)
	default:
		t.Fatalf("Unexpected type for email column: %T", record.Column(2))
	}

	isActiveColumn := record.Column(3).(*array.Boolean)
	joinAtColumn := record.Column(4).(*array.Timestamp)

	// Basic assertions
	assert.Equal(t, users[0].ID, idColumn.Value(0))
	assert.Equal(t, users[0].Name, nameStr)
	assert.Equal(t, users[0].Email, emailStr)
	assert.Equal(t, users[0].IsActive, isActiveColumn.Value(0))

	// Get values for second row
	switch nameCol := record.Column(1).(type) {
	case *array.Dictionary:
		nameIndex := nameCol.GetValueIndex(1)
		nameStr = nameCol.Dictionary().(*array.String).Value(nameIndex)
	case *array.String:
		nameStr = nameCol.Value(1)
	}

	switch emailCol := record.Column(2).(type) {
	case *array.Dictionary:
		emailIndex := emailCol.GetValueIndex(1)
		emailStr = emailCol.Dictionary().(*array.String).Value(emailIndex)
	case *array.String:
		emailStr = emailCol.Value(1)
	}

	assert.Equal(t, users[1].ID, idColumn.Value(1))
	assert.Equal(t, users[1].Name, nameStr)
	assert.Equal(t, users[1].Email, emailStr)
	assert.Equal(t, users[1].IsActive, isActiveColumn.Value(1))

	// Get values for third row
	switch nameCol := record.Column(1).(type) {
	case *array.Dictionary:
		nameIndex := nameCol.GetValueIndex(2)
		nameStr = nameCol.Dictionary().(*array.String).Value(nameIndex)
	case *array.String:
		nameStr = nameCol.Value(2)
	}

	switch emailCol := record.Column(2).(type) {
	case *array.Dictionary:
		emailIndex := emailCol.GetValueIndex(2)
		emailStr = emailCol.Dictionary().(*array.String).Value(emailIndex)
	case *array.String:
		emailStr = emailCol.Value(2)
	}

	assert.Equal(t, users[2].ID, idColumn.Value(2))
	assert.Equal(t, users[2].Name, nameStr)
	assert.Equal(t, users[2].Email, emailStr)
	assert.Equal(t, users[2].IsActive, isActiveColumn.Value(2))

	assert.Equal(t, users[0].JoinedAt.UnixNano(), int64(joinAtColumn.Value(0)))
	assert.Equal(t, users[1].JoinedAt.UnixNano(), int64(joinAtColumn.Value(1)))
	assert.Equal(t, users[2].JoinedAt.UnixNano(), int64(joinAtColumn.Value(2)))

	record.Release()
}

func TestEmptySlice(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	encoder := encode.NewEncoder(schema)

	// Empty slice
	emptyUsers := []User{}
	record, err := encoder.Encode(emptyUsers)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), record.NumRows()) // Verify empty record

	// Verify columns exist
	assert.Equal(t, int64(2), record.NumCols())
	assert.NotNil(t, record.Column(0))
	assert.NotNil(t, record.Column(1))
	record.Release()
}

func TestMapSlice(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	encoder := encode.NewEncoder(schema, encode.WithMode(encode.ModeHighThroughput))
	data := []map[string]interface{}{
		{"id": int64(1), "name": "Alice", "value": 1.23},
		{"id": int64(2), "name": "Bob", "value": 4.56},
		{"id": int64(3), "value": 7.89}, // Missing "Name"
		{"name": "Dave", "value": 0.12}, // Missing ID
	}

	record, err := encoder.Encode(data)
	assert.NoError(t, err)

	assert.Equal(t, int64(4), record.NumRows())

	idColumn := record.Column(0).(*array.Int64)
	nameColumn := record.Column(1).(*array.String)
	valueColumn := record.Column(2).(*array.Float64)

	assert.Equal(t, int64(1), idColumn.Value(0))
	assert.Equal(t, "Alice", nameColumn.Value(0))
	assert.Equal(t, 1.23, valueColumn.Value(0))

	assert.Equal(t, int64(2), idColumn.Value(1))
	assert.Equal(t, "Bob", nameColumn.Value(1))
	assert.Equal(t, 4.56, valueColumn.Value(1))

	assert.Equal(t, int64(3), idColumn.Value(2))
	assert.True(t, nameColumn.IsNull(2)) // Name is missing
	assert.Equal(t, 7.89, valueColumn.Value(2))

	assert.True(t, idColumn.IsNull(3)) //id is missing, so it should be null
	assert.Equal(t, "Dave", nameColumn.Value(3))
	assert.Equal(t, 0.12, valueColumn.Value(3))

	record.Release()
}

func TestNilPointerStruct(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	encoder := encode.NewEncoder(schema)

	type TestData struct {
		ID   int64
		Name *string // Pointer field
	}

	data := []*TestData{
		{ID: 1, Name: stringPtr("Alice")}, // String
		{ID: 2, Name: nil},                // Name is nil
		nil,
	}

	t.Logf("Data: %+v", data)
	record, err := encoder.Encode(data)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), record.NumRows())
	nameColumn := record.Column(1).(*array.String)
	idColumn := record.Column(0).(*array.Int64)

	assert.Equal(t, "Alice", nameColumn.Value(0))
	assert.True(t, nameColumn.IsNull(1)) // Should be null
	assert.True(t, nameColumn.IsNull(2)) //should be null
	assert.True(t, idColumn.IsNull(2))
	record.Release()

	//Now with a regular string.
	encoder = encode.NewEncoder(schema)
	type TestData2 struct {
		ID   int64
		Name string // regular field
	}

	data2 := []*TestData2{
		{ID: 1, Name: "Alice"}, // String
		nil,
	}
	record, err = encoder.Encode(data2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), record.NumRows())
	nameColumn2 := record.Column(1).(*array.String)
	assert.Equal(t, "Alice", nameColumn2.Value(0))
	assert.True(t, nameColumn2.IsNull(1))
	record.Release()
}

func stringPtr(s string) *string {
	return &s
}

func BenchmarkEncodeZeroAlloc(b *testing.B) {
	// Simplified Schema and DataRow for Benchmarking
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	type DataRow struct {
		ID    int64
		Name  string
		Value float64
	}

	// Generate a large dataset for the benchmark
	numRows := 10000
	var data []DataRow
	for i := 0; i < numRows; i++ {
		data = append(data, DataRow{ID: int64(i), Name: fmt.Sprintf("Name%d", i), Value: float64(i) * 1.5})
	}
	encoder := encode.NewEncoder(schema, encode.WithMode(encode.ModeZeroAlloc))
	b.ResetTimer() // Resets the timer to exclude setup time.
	for i := 0; i < b.N; i++ {
		record, err := encoder.Encode(data)
		if err != nil {
			b.Fatal(err)
		}
		// Release the record to free memory
		record.Release()
	}
	b.ReportAllocs()
}

func BenchmarkEncodeHighThroughput(b *testing.B) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(b, 0)
	numRows := 10000

	// Simplified Schema and DataRow for Benchmarking.
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	type DataRow struct {
		ID    int64
		Name  string
		Value float64
	}
	var data []DataRow

	// Generate Dataset
	for i := 0; i < numRows; i++ {
		data = append(data, DataRow{ID: int64(i), Name: fmt.Sprintf("Name%d", i), Value: float64(i) * 1.5})
	}

	// Vary the number of workers for comparison
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("Workers=%d", workers), func(b *testing.B) {
			encoder := encode.NewEncoder(schema, encode.WithMode(encode.ModeHighThroughput), encode.WithAllocator(pool), encode.WithWorkers(workers))
			b.ResetTimer() //reset timer
			for i := 0; i < b.N; i++ {
				record, err := encoder.Encode(data)
				if err != nil {
					b.Fatal(err)
				}
				// Release the record to free memory
				record.Release()
			}
			b.ReportAllocs()
		})
	}
}
