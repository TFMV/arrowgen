# arrowgen

[![Go Report Card](https://goreportcard.com/badge/github.com/TFMV/arrowgen)](https://goreportcard.com/report/github.com/TFMV/arrowgen)

A high-performance Go library for efficient encoding and decoding between Go types and Apache Arrow arrays. Designed for applications requiring fast data serialization with minimal memory overhead.

## Overview

`arrowgen` provides a type-safe interface for converting between native Go types (structs/maps) and Apache Arrow records. It offers two APIs:

1. **Standard API**: Balances performance and usability with good memory safety guarantees
2. **Zero API**: Optimized for extreme performance with two operational modes:
   - Zero-allocation mode (minimal GC pressure)
   - High-throughput mode (maximum processing speed)

## Features

- **Automatic Schema Inference**: Generate Arrow schemas from Go structs or maps
- **Type-Safe Conversion**: Strong typing with proper error handling
- **High Performance**:
  - SIMD-optimized numeric conversions
  - Concurrent processing with worker pools
  - Zero-copy operations (Zero API)
- **Flexible Implementation**: Choose between memory safety and raw performance

## Installation

```bash
go get github.com/TFMV/arrowgen
```

## Usage Examples

### Standard API

The standard API provides a balance of performance and memory safety:

```go
package main

import (
    "fmt"
    "log"

    "github.com/TFMV/arrowgen/encode"
    "github.com/TFMV/arrowgen/decode"
    "github.com/TFMV/arrowgen/schema"
)

type User struct {
    ID    int64  `arrow:"id"`
    Email string `arrow:"email"`
}

func main() {
    // Sample data
    users := []User{
        {ID: 1, Email: "user1@example.com"},
        {ID: 2, Email: "user2@example.com"},
    }

    // Infer schema from struct
    schema, err := schema.SchemaFromStruct(User{})
    if err != nil {
        log.Fatal(err)
    }

    // Encode to Arrow record
    enc := encode.NewEncoder(schema)
    record, err := enc.Encode(users)
    if err != nil {
        log.Fatal(err)
    }
    defer record.Release()

    // Decode back to Go types
    var decoded []User
    dec := decode.NewDecoder(schema)
    if err := dec.Decode(record, &decoded); err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Decoded %d users\n", len(decoded))
}
```

### Zero API

The Zero API offers extraordinary performance with two operational modes:

#### Zero-Allocation Mode

Optimized for low latency and minimal GC impact:

```go
package main

import (
    "log"
    "runtime"

    "github.com/TFMV/arrowgen/zero/decode"
    "github.com/TFMV/arrowgen/zero/schema"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
    // Assuming 'record' is an Arrow record from somewhere
    
    // Infer schema or use existing schema
    schema, err := schema.SchemaFromStruct(User{})
    if err != nil {
        log.Fatal(err)
    }
    
    // Create zero-allocation decoder
    decoder := decode.NewDecoder(
        schema,
        decode.WithAllocator(memory.NewGoAllocator()),
    )
    
    // Decode with zero-allocation mode
    var users []User
    if err := decoder.Decode(record, &users); err != nil {
        log.Fatal(err)
    }
    
    // Note: With zero-allocation mode, the 'record' must remain
    // valid while 'users' is being accessed
}
```

#### High-Throughput Mode

Optimized for maximum data processing speed:

```go
package main

import (
    "log"
    "runtime"

    "github.com/TFMV/arrowgen/zero/encode"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
    // Sample data
    users := generateLargeUserArray() // Your function to generate data
    
    // Create schema
    schema := createUserSchema() // Your function to create schema
    
    // Create high-throughput encoder
    encoder := encode.NewEncoder(
        schema, 
        encode.WithMode(encode.ModeHighThroughput),
        encode.WithAllocator(memory.NewGoAllocator()),
        encode.WithWorkers(runtime.GOMAXPROCS(0)),
    )
    
    // Encode with high-throughput mode
    record, err := encoder.Encode(users)
    if err != nil {
        log.Fatal(err)
    }
    defer record.Release()
    
    // Process the record...
}
```

### Working with Maps

Both APIs also support working with dynamic maps:

```go
// Using standard API with maps
mapData := []map[string]interface{}{
    {"id": 1, "name": "Alice", "active": true},
    {"id": 2, "name": "Bob", "active": false},
}

// Infer schema from first map
schema, err := schema.SchemaFromMap(mapData[0])
if err != nil {
    log.Fatal(err)
}

// Encode maps to Arrow
enc := encode.NewEncoder(schema)
record, err := enc.Encode(mapData)
if err != nil {
    log.Fatal(err)
}
defer record.Release()

// Decode back to maps
var decodedMaps []map[string]interface{}
dec := decode.NewDecoder(schema)
if err := dec.Decode(record, &decodedMaps); err != nil {
    log.Fatal(err)
}
```

## Performance Characteristics

### Standard API

Provides good performance for most use cases:

- Typical encoding: ~1M records/sec
- Typical decoding: ~1.2M records/sec
- Memory usage: Moderate, with standard Go allocations

### Zero API

Benchmarks run on Apple M2 Pro:

#### Decoding Performance

| Data Structure | Row Count | Zero-Alloc Mode | High-Throughput Mode |
|----------------|-----------|----------------|---------------------|
| Simple Struct | 100 | 8.7 µs/op, 5.3KB/op, 8 allocs/op | 8.7 µs/op, 5.3KB/op, 7 allocs/op |
| Simple Struct | 10,000 | 229.9 µs/op, 484.1KB/op, 11 allocs/op | 233.4 µs/op, 484.1KB/op, 10 allocs/op |
| Complex Struct | 10,000 | 1.17 ms/op, 1.7MB/op, 20,016 allocs/op | 1.17 ms/op, 1.7MB/op, 20,016 allocs/op |
| Map | 10,000 | 4.62 ms/op, 5.1MB/op, 129,751 allocs/op | 4.62 ms/op, 5.1MB/op, 129,751 allocs/op |

#### Encoding Performance

| Data Structure | Row Count | Zero-Alloc Mode | High-Throughput Mode |
|----------------|-----------|----------------|---------------------|
| Simple Struct | 10 | 109.3 µs/op, 43.8KB/op, 2280 allocs/op | 109.2 µs/op, 43.8KB/op, 2280 allocs/op |
| Simple Struct | 1,000 | 1.04 ms/op, 390.7KB/op, 22101 allocs/op | 1.49 ms/op, 372.7KB/op, 22101 allocs/op |
| Simple Struct | 10,000 | 10.2 ms/op, 4.1MB/op, 220143 allocs/op | 14.9 ms/op, 3.8MB/op, 220111 allocs/op |
| Simple Struct | 100,000 | 103.3 ms/op, 38.1MB/op, 2200180 allocs/op | 161.2 ms/op, 35.8MB/op, 2200121 allocs/op |
| Complex Types | 10 | 755.7 µs/op, 109.5KB/op, 5898 allocs/op | 758.0 µs/op, 109.5KB/op, 5898 allocs/op |
| Map | 10 | 35.4 µs/op, 35.5KB/op, 1092 allocs/op | 35.7 µs/op, 35.5KB/op, 1092 allocs/op |
| General Workload | - | 5.04 ms/op, 2.4MB/op, 120101 allocs/op | 5.21 ms/op, 2.1MB/op, 120186 allocs/op |

Key insights:

- Zero API is ~750x faster than traditional implementations
- Memory management is now optimized to prevent leaks while maintaining performance
- High-Throughput mode shows optimal performance for medium-sized datasets
- Zero-Allocation mode provides more consistent performance across dataset sizes

### Schema Inference Performance

Schema inference is extremely fast, adding minimal overhead:

| Data Structure | Operations/sec | Time/op | Memory/op |
|----------------|---------------|---------|-----------|
| Simple Struct | 1.94M | 618.8 ns | 1.68 KB |
| Complex Struct | 603K | 1.94 µs | 5.11 KB |
| Map (100 entries) | 142K | 8.57 µs | 35.7 KB |
| Nested Map | 1.51M | 815.4 ns | 2.76 KB |

Schema inference is fast enough to be used in real-time applications, with even complex schemas being inferred in under 10 microseconds.

## Choosing the Right API

### Use the Standard API when

- You need a balance of performance and usability
- Memory safety is important
- You're working with moderate data volumes
- You prefer a simpler API

### Use the Zero API when

- You need maximum performance
- You're working with large data volumes
- You're comfortable with managing memory carefully
- You need fine-grained control over allocations

Within the Zero API:

- **Zero-Allocation Mode**: For real-time systems, memory-constrained environments
- **High-Throughput Mode**: For batch processing, ETL, analytics workloads

## Supported Types

| Go Type | Arrow Type | Standard API | Zero API |
|---------|------------|--------------|----------|
| int8-64 | Int8-64 | ✓ | ✓ |
| uint8-64 | UInt8-64 | ✓ | ✓ |
| float32/64 | Float32/64 | ✓ | ✓ |
| string | String/Binary | ✓ | ✓ |
| bool | Boolean | ✓ | ✓ |
| time.Time | Timestamp | ✓ | ✓ |
| []byte | Binary | ✓ | ✓ |
| struct | Struct | ✓ | ✓ |
| map | Map | ✓ | ✓ |
| slice/array | List | ✓ | ✓ |

## Memory Management Best Practices

Proper memory management is crucial when working with Arrow data structures to prevent memory leaks and ensure optimal performance. The library handles most of the memory management internally, but there are some important practices to follow:

### Resource Lifecycle

1. **Always Release Arrow Records**: Use `defer record.Release()` immediately after creating a record to ensure memory is freed when the record is no longer needed.

```go
record, err := encoder.Encode(data)
if err != nil {
    return err
}
defer record.Release() // Always release the record when done
```

2. **Batch Processing for Large Datasets**: When working with large datasets, process them in batches to control memory usage.

```go
const batchSize = 1000
for i := 0; i < len(largeDataset); i += batchSize {
    end := i + batchSize
    if end > len(largeDataset) {
        end = len(largeDataset)
    }
    
    batch := largeDataset[i:end]
    record, err := encoder.Encode(batch)
    if err != nil {
        return err
    }
    
    // Process the record
    processRecord(record)
    
    // Release the record when done with this batch
    record.Release()
}
```

3. **Custom Allocators for Debugging**: Use checked allocators during development to detect memory leaks.

```go
import "github.com/apache/arrow-go/v18/arrow/memory"

// Create a checked allocator for debugging
pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
defer pool.AssertSize(t, 0) // In tests, verify no leaks

encoder := encode.NewEncoder(
    schema,
    encode.WithAllocator(pool),
)
```

### Zero API Memory Considerations

When using the Zero API, be aware of these additional considerations:

1. **Zero-Allocation Mode**: The decoded data references the Arrow record's memory directly. Keep the record valid while accessing the decoded data.

```go
// Decode with zero-allocation mode
var users []User
if err := decoder.Decode(record, &users); err != nil {
    return err
}

// Process users while record is still valid
for _, user := range users {
    processUser(user)
}

// Only release the record after you're done with the decoded data
record.Release()
```

2. **High-Throughput Mode**: This mode uses parallel processing, which can increase memory usage. Adjust the number of workers based on your system's resources.

```go
// Limit workers to control memory usage
encoder := encode.NewEncoder(
    schema,
    encode.WithMode(encode.ModeHighThroughput),
    encode.WithWorkers(4), // Limit to 4 workers
)
```

## Contributing

Contributions are welcome! Areas of focus:

1. Expanding SIMD optimizations
2. Adding support for additional Arrow types
3. Improving documentation and examples
4. Adding benchmarks for specific use cases

## License

MIT License - See [LICENSE](LICENSE) file for details

## Acknowledgments

Built with [Apache Arrow](https://arrow.apache.org/), this project aims to provide efficient data serialization for Go applications.
