package examples

import (
	"fmt"
	"log"

	"github.com/TFMV/arrowgen/zero/decode"
	"github.com/TFMV/arrowgen/zero/encode"
	"github.com/TFMV/arrowgen/zero/schema"
)

// User represents a basic struct for demonstration
type User struct {
	ID       int64  `arrow:"id"`
	Name     string `arrow:"name"`
	Email    string `arrow:"email"`
	IsActive bool   `arrow:"is_active"`
}

// BasicUsage demonstrates the most basic usage of the Zero API
// with automatic schema inference and zero-allocation mode
func BasicUsage() {
	// Create sample data
	users := []User{
		{ID: 1, Name: "Alice", Email: "alice@example.com", IsActive: true},
		{ID: 2, Name: "Bob", Email: "bob@example.com", IsActive: false},
		{ID: 3, Name: "Charlie", Email: "charlie@example.com", IsActive: true},
	}

	// Step 1: Infer schema from struct
	fmt.Println("Step 1: Inferring schema from struct")
	schema, err := schema.SchemaFromStruct(User{})
	if err != nil {
		log.Fatalf("Failed to infer schema: %v", err)
	}
	fmt.Printf("Inferred schema with %d fields: %v\n", len(schema.Fields()), schema)

	// Step 2: Create encoder with default settings (zero-allocation mode)
	fmt.Println("\nStep 2: Creating encoder with zero-allocation mode")
	encoder := encode.NewEncoder(schema)

	// Step 3: Encode data to Arrow record
	fmt.Println("Step 3: Encoding data to Arrow record")
	record, err := encoder.Encode(users)
	if err != nil {
		log.Fatalf("Failed to encode data: %v", err)
	}
	defer record.Release()

	fmt.Printf("Encoded %d rows with %d columns\n", record.NumRows(), record.NumCols())

	// Step 4: Create decoder with default settings
	fmt.Println("\nStep 4: Creating decoder with zero-allocation mode")
	decoder := decode.NewDecoder(schema)

	// Step 5: Decode Arrow record back to Go struct
	fmt.Println("Step 5: Decoding Arrow record back to Go struct")
	var decodedUsers []User
	if err := decoder.Decode(record, &decodedUsers); err != nil {
		log.Fatalf("Failed to decode data: %v", err)
	}

	// Step 6: Verify the results
	fmt.Println("\nStep 6: Verifying results")
	fmt.Printf("Decoded %d users\n", len(decodedUsers))
	for i, user := range decodedUsers {
		fmt.Printf("User %d: ID=%d, Name=%s, Email=%s, IsActive=%v\n",
			i+1, user.ID, user.Name, user.Email, user.IsActive)
	}

	// Important: In zero-allocation mode, the record must remain valid
	// while the decoded data is being accessed
	fmt.Println("\nNote: In zero-allocation mode, the Arrow record must remain valid")
	fmt.Println("while the decoded data is being accessed.")
}
