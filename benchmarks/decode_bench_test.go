package benchmarks

import (
	"testing"
	"time"

	"github.com/TFMV/arrowgen/decode"
	"github.com/TFMV/arrowgen/encode"
	"github.com/TFMV/arrowgen/schema"
)

func BenchmarkDecodeUsers(b *testing.B) {
	arrowSchema, err := schema.SchemaFromStruct(User{})
	if err != nil {
		b.Fatal(err)
	}

	// Create sample data
	users := make([]User, 100000)
	for i := 0; i < 100000; i++ {
		users[i] = User{
			ID:       int64(i),
			Email:    "user@example.com",
			JoinedAt: time.Now(),
		}
	}

	// Encode once to create our test data
	enc := encode.NewEncoder(arrowSchema)
	record, err := enc.Encode(users)
	if err != nil {
		b.Fatal(err)
	}
	defer record.Release()

	// Setup decoder
	dec := decode.NewDecoder(arrowSchema)
	var result []User

	// Run benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = result[:0] // Reset slice without deallocating
		err := dec.Decode(record, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}
