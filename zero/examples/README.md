# Zero API Examples

This directory contains examples demonstrating the key features and usage patterns of the Zero API in arrowgen.

## Overview

The Zero API provides high-performance encoding and decoding between Go types and Apache Arrow records with two operational modes:

1. **Zero-Allocation Mode**: Optimized for minimal memory allocations and GC pressure
2. **High-Throughput Mode**: Optimized for maximum processing speed with parallel execution

## Running the Examples

To run all examples:

```bash
go run main.go 0
```

To run a specific example:

```bash
go run main.go <example-number>
```

Or simply run without arguments and follow the prompts:

```bash
go run main.go
```

## Example Descriptions

### 1. Basic Usage (`01_basic_usage.go`)

Demonstrates the most basic usage of the Zero API with automatic schema inference and zero-allocation mode. This example shows:

- Inferring Arrow schema from Go struct
- Encoding Go structs to Arrow records
- Decoding Arrow records back to Go structs
- Zero-allocation mode basics

### 2. High-Throughput Mode (`02_high_throughput.go`)

Demonstrates the high-throughput mode of the Zero API, which is optimized for maximum processing speed with parallel execution. This example shows:

- Creating an encoder with high-throughput mode
- Configuring worker pools for parallel processing
- Performance comparison with zero-allocation mode
- Handling large datasets efficiently

### 3. Dynamic Maps (`03_dynamic_maps.go`)

Demonstrates working with dynamic map data and schema inference. This example shows:

- Inferring schema from map data
- Encoding and decoding maps with various value types
- Handling nested maps
- Dynamic schema generation

### 4. Schema Options (`04_schema_options.go`)

Demonstrates various schema inference options and customizations. This example shows:

- Configuring schema options (nullable fields, dictionary encoding, etc.)
- Custom type mappings
- Schema metadata
- Advanced schema configuration

### 5. Memory Management (`05_memory_management.go`)

Demonstrates memory management and custom allocators. This example shows:

- Using custom memory allocators
- Memory-efficient batch processing
- Memory management best practices
- Monitoring memory usage

## Key Concepts

Throughout these examples, you'll see several key concepts demonstrated:

1. **Schema Inference**: Automatically generating Arrow schemas from Go types
2. **Zero-Allocation Mode**: Minimizing memory allocations for performance-critical applications
3. **High-Throughput Mode**: Maximizing processing speed with parallel execution
4. **Memory Management**: Controlling memory usage with custom allocators and batch processing
5. **Type Safety**: Maintaining type safety while achieving high performance

## Performance Considerations

The examples include performance measurements to demonstrate the efficiency of the Zero API. Key performance characteristics include:

- Zero-allocation mode minimizes GC pressure
- High-throughput mode maximizes processing speed
- Memory usage scales linearly with data size
- Batch processing for efficient memory management

## Additional Resources

For more information, refer to:

- [Zero API Documentation](../README.md)
- [arrowgen Main Documentation](../../README.md)
- [Apache Arrow Documentation](https://arrow.apache.org/docs/)
