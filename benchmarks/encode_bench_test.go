package benchmarks

import (
	"testing"
	"time"

	"github.com/TFMV/arrowgen/encode"
	"github.com/TFMV/arrowgen/schema"
)

type User struct {
	ID       int64     `arrow:"id"`
	Email    string    `arrow:"email"`
	JoinedAt time.Time `arrow:"joined_at"`
}

func BenchmarkEncodeUsers(b *testing.B) {
	arrowSchema, err := schema.SchemaFromStruct(User{})
	if err != nil {
		b.Fatal(err)
	}

	// Create a large slice of users.
	var users []User
	for i := 0; i < 100000; i++ {
		users = append(users, User{
			ID:       int64(i),
			Email:    "user@example.com",
			JoinedAt: time.Now(),
		})
	}

	enc := encode.NewEncoder(arrowSchema)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, err := enc.Encode(users)
		if err != nil {
			b.Fatal(err)
		}
		record.Release()
	}
}
