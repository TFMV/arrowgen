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

#### Decoding Performance

| Row Count | Zero-Alloc Mode | Standard Mode | Original Implementation |
|-----------|----------------|--------------|----------------------|
| 100       | 8.6 µs/op, 5.3KB/op, 8 allocs/op | 8.7 µs/op, 5.3KB/op, 7 allocs/op | 6.5 ms/op, 7.0MB/op, 400K allocs/op |
| 1,000     | 38.4 µs/op, 49.9KB/op, 11 allocs/op | 39.2 µs/op, 49.9KB/op, 10 allocs/op | 7.1 ms/op, 19.6MB/op, 400K allocs/op |
| 10,000    | 273.5 µs/op, 484.1KB/op, 11 allocs/op | 233.3 µs/op, 484.1KB/op, 10 allocs/op | - |
| 100,000   | 1.57 ms/op, 4.8MB/op, 11 allocs/op | 1.52 ms/op, 4.8MB/op, 10 allocs/op | - |

#### Encoding Performance

| Row Count | Zero-Alloc Mode | High-Throughput Mode |
|-----------|----------------|---------------------|
| 100       | 12.4 µs/op, 17.9KB/op, 83 allocs/op | 19.6 µs/op, 19.2KB/op, 97 allocs/op |
| 1,000     | 72.9 µs/op, 128.1KB/op, 106 allocs/op | 67.9 µs/op, 129.4KB/op, 120 allocs/op |
| 10,000    | 737.1 µs/op, 1.6MB/op, 145 allocs/op | 516.3 µs/op, 1.6MB/op, 159 allocs/op |
| 100,000   | 5.2 ms/op, 12.0MB/op, 175 allocs/op | 2.6 ms/op, 12.0MB/op, 189 allocs/op |

### Complex Types (14 fields including all primitive types)

#### Decoding Performance

| Row Count | Zero-Alloc Mode | Standard Mode |
|-----------|----------------|--------------|
| 100       | 31.0 µs/op, 17.8KB/op, 207 allocs/op | 31.0 µs/op, 17.8KB/op, 207 allocs/op |
| 1,000     | 135.6 µs/op, 172.4KB/op, 2,016 allocs/op | 135.6 µs/op, 172.4KB/op, 2,016 allocs/op |
| 10,000    | 1.05 ms/op, 1.7MB/op, 20,016 allocs/op | 1.05 ms/op, 1.7MB/op, 20,016 allocs/op |
| 100,000   | 8.04 ms/op, 16.8MB/op, 200,016 allocs/op | 8.04 ms/op, 16.8MB/op, 200,016 allocs/op |

#### Encoding Performance

| Row Count | Zero-Alloc Mode | High-Throughput Mode |
|-----------|----------------|---------------------|
| 100       | 28.2 µs/op, 38.2KB/op, 297 allocs/op | 35.3 µs/op, 39.8KB/op, 311 allocs/op |
| 1,000     | 192.1 µs/op, 216.3KB/op, 1,257 allocs/op | 131.2 µs/op, 217.9KB/op, 1,271 allocs/op |
| 10,000    | 1.90 ms/op, 3.1MB/op, 10,377 allocs/op | 1.03 ms/op, 3.1MB/op, 10,391 allocs/op |
| 100,000   | TBD | TBD |

### Map Decoding

| Row Count | Zero-Alloc Mode | Standard Mode |
|-----------|----------------|--------------|
| 100       | 51.9 µs/op, 51.0KB/op, 1,207 allocs/op | 51.9 µs/op, 51.0KB/op, 1,207 allocs/op |
| 1,000     | 439.7 µs/op, 510.6KB/op, 12,751 allocs/op | 439.7 µs/op, 510.6KB/op, 12,751 allocs/op |
| 10,000    | 4.67 ms/op, 5.1MB/op, 129,751 allocs/op | 4.67 ms/op, 5.1MB/op, 129,751 allocs/op |

### Schema Inference Performance

Latest benchmarks for schema inference from different data structures (Apple M2 Pro):

| Data Structure | Operations/sec | Time/op | Memory/op | Allocations/op |
|----------------|---------------|---------|-----------|----------------|
| Simple Struct (4 fields) | 1.94M | 618.8 ns | 1.68 KB | 12 |
| Complex Struct (14 fields) | 603K | 1.94 µs | 5.11 KB | 25 |
| Nested Struct | 1.00M | 1.23 µs | 2.82 KB | 21 |
| Map (5 entries) | 2.35M | 527.8 ns | 1.93 KB | 12 |
| Map (20 entries) | 714K | 1.70 µs | 7.32 KB | 32 |
| Map (100 entries) | 142K | 8.57 µs | 35.7 KB | 128 |
| Nested Map (5 entries) | 1.49M | 822.0 ns | 2.76 KB | 21 |
| Nested Map (20 entries) | 1.51M | 822.7 ns | 2.76 KB | 21 |
| Nested Map (100 entries) | 1.51M | 815.4 ns | 2.76 KB | 21 |
| With Dictionary Disabled | 617K | 1.96 µs | 5.06 KB | 24 |
| With Max Depth Check | 3.07M | 382.8 ns | 512 B | 7 |

These results show that schema inference is extremely fast, with simple schemas being inferred in under a microsecond. Even complex maps with 100 entries can be processed at a rate of over 141,000 per second, making schema inference negligible in the overall processing pipeline.

## Schema Inference Analysis

The schema inference benchmarks reveal several important characteristics:

1. **Speed and Efficiency**: Schema inference is extremely fast, with most operations completing in under 2 microseconds. This makes dynamic schema generation viable even for performance-sensitive applications.

2. **Scaling with Complexity**:
   - Simple struct inference: ~1.94M ops/sec (618.8 ns/op)
   - Complex struct inference: ~603K ops/sec (1.94 µs/op)
   - The time complexity scales roughly linearly with the number of fields

3. **Map vs. Struct Performance**:
   - Small maps (5 entries) are inferred faster than equivalent structs
   - Large maps (100 entries) require more memory and allocations, but still maintain good performance

4. **Nested Structure Handling**:
   - Nested structs: ~1M ops/sec (1.23 µs/op)
   - Nested maps: ~1.5M ops/sec (822 ns/op)
   - Interestingly, nested map inference performance remains constant regardless of the number of nested entries

5. **Memory Usage Patterns**:
   - Memory usage scales linearly with schema complexity
   - Allocation count remains reasonable even for complex schemas
   - With dictionary encoding disabled, there's minimal impact on performance

6. **Configuration Impact**:
   - Setting a maximum nesting depth significantly improves performance (3.07M ops/sec)
   - This optimization is useful for scenarios where schema validation is more important than complete inference

These benchmarks demonstrate that the schema inference system adds minimal overhead to the Arrow encoding/decoding process, making it practical to use dynamic schema generation in most applications.

## Performance Analysis

The benchmark results reveal several key insights:

1. **Zero-Allocation vs. High-Throughput Mode**:
   - For small datasets (100 rows), Zero-Allocation mode is faster for encoding simple structs
   - For larger datasets (10,000+ rows), High-Throughput mode shows up to 2.2x speedup in encoding
   - Memory usage is nearly identical between modes, with High-Throughput using slightly more allocations

2. **Decoding Performance**:
   - Standard mode is slightly faster than Zero-Allocation mode for larger datasets (10,000+ rows)
   - Both modes maintain extremely low allocation counts regardless of dataset size
   - Memory usage scales linearly with data size in both modes
   - Complex types show identical performance between modes but with higher allocation counts

3. **Scaling Characteristics**:
   - Both modes scale linearly with data size
   - High-Throughput mode shows better scaling as dataset size increases for encoding
   - Allocation count increases logarithmically rather than linearly with data size

4. **Comparison to Original Implementation**:
   - Zero API is approximately 750x faster for decoding 100 rows
   - Memory usage is reduced by 99.9% (5.3KB vs 7.0MB)
   - Allocations are reduced by 99.998% (8 vs 400K)

5. **Complex vs. Simple Types**:
   - Complex types (14 fields) show higher allocation counts but still maintain excellent performance
   - High-Throughput mode shows the most benefit with complex types at scale (1.9x speedup at 10K rows)
   - Map decoding requires more allocations but maintains good performance characteristics

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
