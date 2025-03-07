# arrowgen

A high-performance Go library for zero-copy encoding/decoding between native Go types (structs/maps) and Apache Arrow arrays.

## Features

- **Performance-centric:** Zero-copy design with minimal allocations.
- **Concurrency-first:** Leverages goroutines and pooling.
- **Usability:** Simple API for schema inference, encoding, and decoding.
- **Flexibility:** Supports native structs and generic maps.

## Performance

Benchmarks run on Apple M2 Pro (10 cores) with 10,000 records:

```
BenchmarkEncodeUsers-10    1588 ops    782μs/op    2.4MB/op    30K allocs/op
BenchmarkDecodeUsers-10    1450 ops    785μs/op    1.1MB/op    39K allocs/op
```

Key Performance Characteristics:

- Encoding processes ~1.3M records/second
- Decoding processes ~1.3M records/second
- Memory usage scales linearly with record count
- Thread-safe and concurrent processing ready
- Efficient memory management with pooling and SIMD optimizations

Note: Performance may vary based on hardware, data types, and record complexity.

## Example

```go
package main

import (
 "fmt"
 "log"

 "github.com/yourusername/arrowgen/encode"
 "github.com/yourusername/arrowgen/decode"
 "github.com/yourusername/arrowgen/schema"
 "github.com/apache/arrow/go/arrow/array"
)

type User struct {
 ID    int64  `arrow:"id"`
 Email string `arrow:"email"`
}

func main() {
 // Infer Arrow schema from struct.
 arrowSchema, err := schema.SchemaFromStruct(User{})
 if err != nil {
  log.Fatal(err)
 }

 // Sample data.
 users := []User{
  {ID: 1, Email: "user1@example.com"},
  {ID: 2, Email: "user2@example.com"},
 }

 // Encode the slice of Users into an Arrow record.
 enc := encode.NewEncoder(arrowSchema)
 record, err := enc.Encode(users)
 if err != nil {
  log.Fatal(err)
 }
 defer record.Release()

 // Decode the Arrow record back into Go types.
 dec := decode.NewDecoder(arrowSchema)
 var decoded []User
 err = dec.Decode(record, &decoded)
 if err != nil {
  log.Fatal(err)
 }

 fmt.Printf("Decoded: %+v\n", decoded)
}
```

## License

MIT License
