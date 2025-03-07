# Zero-Copy Arrow Decoder

A high-performance implementation of Arrow record decoding with two operational modes: zero-allocation and high-throughput.

## Operational Modes

### 1. Zero-Allocation Mode

Optimized for scenarios where memory pressure and GC pauses must be minimized:

- Real-time systems
- Memory-constrained environments
- Systems with strict GC requirements
- Low-latency applications

### 2. High-Throughput Mode

Optimized for maximum throughput when memory usage is less critical:

- Batch processing
- ETL pipelines
- Data warehousing
- Analytics workloads

## Performance Comparison

Benchmarks run on Apple M2 Pro comparing different modes and implementations:

### Simple Struct (4 fields: int64, string, string, int32)

| Row Count | Zero-Alloc Mode | High-Throughput Mode | Original Implementation |
|-----------|----------------|---------------------|----------------------|
| 100       | 8.7 µs/op, 5.3KB/op, 7 allocs/op | TBD | 6.5 ms/op, 7.0MB/op, 400K allocs/op |
| 1,000     | 38.8 µs/op, 49.9KB/op, 10 allocs/op | TBD | 7.1 ms/op, 19.6MB/op, 400K allocs/op |
| 10,000    | 248.3 µs/op, 484KB/op, 10 allocs/op | TBD | - |
| 100,000   | 1.6 ms/op, 4.8MB/op, 10 allocs/op | TBD | - |

### Complex Types (14 fields including all primitive types)

| Row Count | Zero-Alloc Mode | High-Throughput Mode |
|-----------|----------------|---------------------|
| 100       | 31.8 µs/op, 17.8KB/op, 207 allocs/op | TBD |
| 1,000     | 130.7 µs/op, 172.4KB/op, 2016 allocs/op | TBD |
| 10,000    | 1.1 ms/op, 1.7MB/op, 20016 allocs/op | TBD |
| 100,000   | 8.1 ms/op, 16.8MB/op, 200016 allocs/op | TBD |

### Map Decoding

| Row Count | Zero-Alloc Mode | High-Throughput Mode |
|-----------|----------------|---------------------|
| 100       | 50.9 µs/op, 51KB/op, 1207 allocs/op | TBD |
| 1,000     | 422.2 µs/op, 511KB/op, 12751 allocs/op | TBD |
| 10,000    | 4.7 ms/op, 5.1MB/op, 129751 allocs/op | TBD |

### Schema Inference Performance

Benchmarks for schema inference from different data structures:

| Data Structure | Operations/sec | Time/op | Memory/op | Allocations/op |
|----------------|---------------|---------|-----------|----------------|
| Simple Struct (4 fields) | 1.92M | 626.5 ns | 1.7 KB | 12 |
| Complex Struct (14 fields) | 606K | 1.97 µs | 5.1 KB | 25 |
| Nested Struct | 1.0M | 1.22 µs | 2.8 KB | 21 |
| Map (5 entries) | 2.34M | 528.3 ns | 1.9 KB | 12 |
| Map (20 entries) | 694K | 1.85 µs | 7.3 KB | 32 |
| Map (100 entries) | 141K | 8.64 µs | 34.9 KB | 128 |
| Nested Map (5 entries) | 1.44M | 984.5 ns | 2.7 KB | 21 |

These results show that schema inference is extremely fast, with simple schemas being inferred in under a microsecond. Even complex maps with 100 entries can be processed at a rate of over 141,000 per second, making schema inference negligible in the overall processing pipeline.

## Mode Characteristics

### Zero-Allocation Mode

- Direct mapping of Arrow memory to Go types
- No intermediate value slices
- Single-threaded map processing
- Minimal memory allocations
- Ideal for latency-sensitive applications

### High-Throughput Mode

- Parallel processing with worker pools
- Pre-allocated buffer pools
- Concurrent map processing with sharding
- Higher memory usage but faster processing
- Ideal for batch processing

## Usage

```go
// Zero-Allocation Mode
decoder := decode.NewDecoder(schema, decode.WithZeroAlloc())

// High-Throughput Mode
decoder := decode.NewDecoder(schema, decode.WithHighThroughput(
    decode.WithWorkerPool(runtime.GOMAXPROCS(0)),
    decode.WithBufferPool(1024),
))

// Decode with either mode
var users []User
if err := decoder.Decode(record, &users); err != nil {
    log.Fatal(err)
}
```

## Implementation Details

### Supported Types

| Go Type | Arrow Type | Zero-Alloc Notes | High-Throughput Notes |
|---------|------------|-----------------|---------------------|
| int8-64 | Int8-64 | Zero-copy conversion | SIMD-optimized batching |
| uint8-64 | UInt8-64 | Zero-copy conversion | SIMD-optimized batching |
| float32/64 | Float32/64 | Zero-copy conversion | SIMD-optimized batching |
| string | String/Binary | Unsafe conversions | Buffered copying |
| bool | Boolean | Direct mapping | Bitset operations |
| time.Time | Timestamp | Direct conversion | Batched conversion |
| []byte | Binary | Zero-copy slicing | Buffered copying |

## Memory Safety

### Zero-Allocation Mode

- Uses unsafe operations for string/byte conversions
- Requires Arrow data to remain valid
- No data modifications allowed

### High-Throughput Mode

- Safe memory operations with copying
- Independent of Arrow data lifetime
- Allows data modifications

## Mode Selection Guide

Choose Zero-Allocation Mode when:

- Memory pressure is a concern
- Low latency is critical
- GC pauses must be minimized
- Data is read-only

Choose High-Throughput Mode when:

- Processing large batches
- Memory usage is less critical
- Maximum throughput is needed
- Data modifications are required

## Future Improvements

1. **High-Throughput Optimizations**
   - Implement adaptive worker pool sizing
   - Add columnar processing for batch operations
   - Optimize buffer pool sizing strategies

2. **Zero-Allocation Enhancements**
   - Extend zero-copy to more complex types
   - Implement zero-allocation dictionary encoding
   - Add memory pressure monitoring

3. **Schema Inference Optimizations**
   - Cache schema inference results for repeated types
   - Implement parallel schema inference for large maps
   - Add schema validation options
   - Support custom type mappings
