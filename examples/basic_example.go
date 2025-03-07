package main

import (
	"fmt"
	"log"
	"time"

	"github.com/TFMV/arrowgen/decode"
	"github.com/TFMV/arrowgen/encode"
	"github.com/TFMV/arrowgen/schema"
)

type User struct {
	ID       int64     `arrow:"id"`
	Email    string    `arrow:"email"`
	JoinedAt time.Time `arrow:"joined_at"`
}

func main() {
	// Step 1: Infer the schema from the User struct.
	arrowSchema, err := schema.SchemaFromStruct(User{})
	if err != nil {
		log.Fatalf("failed to infer schema: %v", err)
	}

	// Step 2: Create sample data.
	users := []User{
		{ID: 1, Email: "alice@example.com", JoinedAt: time.Now()},
		{ID: 2, Email: "bob@example.com", JoinedAt: time.Now()},
	}

	// Step 3: Encode the data into an Arrow record.
	encoder := encode.NewEncoder(arrowSchema)
	record, err := encoder.Encode(users)
	if err != nil {
		log.Fatalf("failed to encode: %v", err)
	}
	defer record.Release()

	fmt.Printf("Encoded record with %d rows and %d columns.\n", record.NumRows(), record.NumCols())

	// Step 4: Decode the Arrow record back into Go structs.
	decoder := decode.NewDecoder(arrowSchema)
	var decodedUsers []User
	if err := decoder.Decode(record, &decodedUsers); err != nil {
		log.Fatalf("failed to decode: %v", err)
	}

	fmt.Printf("Decoded users: %+v\n", decodedUsers)
}
