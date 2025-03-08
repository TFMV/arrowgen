package examples

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/TFMV/arrowgen/zero/decode"
	"github.com/TFMV/arrowgen/zero/encode"
	"github.com/TFMV/arrowgen/zero/schema"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// LogEntry represents a log entry for memory management demonstration
type LogEntry struct {
	Timestamp time.Time `arrow:"timestamp"`
	Level     string    `arrow:"level"`
	Message   string    `arrow:"message"`
	Source    string    `arrow:"source"`
	RequestID string    `arrow:"request_id"`
	UserID    int64     `arrow:"user_id"`
	Duration  float64   `arrow:"duration_ms"`
}

// generateLogEntries creates a dataset of log entries for demonstration
func generateLogEntries(count int) []LogEntry {
	entries := make([]LogEntry, count)
	now := time.Now()
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	sources := []string{"api", "database", "auth", "worker", "cache"}

	for i := 0; i < count; i++ {
		level := levels[i%len(levels)]
		source := sources[i%len(sources)]

		entries[i] = LogEntry{
			Timestamp: now.Add(-time.Duration(i) * time.Second),
			Level:     level,
			Message:   fmt.Sprintf("This is log message #%d from %s with level %s", i, source, level),
			Source:    source,
			RequestID: fmt.Sprintf("req-%d-%d", i, now.Unix()),
			UserID:    int64(i % 1000),
			Duration:  float64(i%100) + 0.5,
		}
	}

	return entries
}

// MemoryManagement demonstrates memory management and custom allocators
func MemoryManagement() {
	// Step 1: Generate sample data
	fmt.Println("Step 1: Generating sample data (10,000 log entries)")
	logs := generateLogEntries(10000)
	fmt.Printf("Generated %d log entries\n", len(logs))

	// Step 2: Infer schema
	fmt.Println("\nStep 2: Inferring schema")
	schema, err := schema.SchemaFromStruct(LogEntry{})
	if err != nil {
		log.Fatalf("Failed to infer schema: %v", err)
	}
	fmt.Printf("Inferred schema with %d fields\n", len(schema.Fields()))

	// Step 3: Using default allocator
	fmt.Println("\nStep 3: Using default allocator")
	defaultEncoder := encode.NewEncoder(schema)

	startTime := time.Now()
	defaultRecord, err := defaultEncoder.Encode(logs)
	if err != nil {
		log.Fatalf("Failed to encode with default allocator: %v", err)
	}
	defer defaultRecord.Release()
	defaultTime := time.Since(startTime)

	fmt.Printf("Encoded with default allocator in %v\n", defaultTime)

	// Print memory stats before custom allocator
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	fmt.Printf("Heap allocations: %d bytes\n", memStats.HeapAlloc)

	// Step 4: Using custom allocator
	fmt.Println("\nStep 4: Using custom allocator")

	// Create a custom memory pool
	customAllocator := memory.NewGoAllocator()

	// Create encoder with custom allocator
	customEncoder := encode.NewEncoder(
		schema,
		encode.WithAllocator(customAllocator),
	)

	startTime = time.Now()
	customRecord, err := customEncoder.Encode(logs)
	if err != nil {
		log.Fatalf("Failed to encode with custom allocator: %v", err)
	}
	defer customRecord.Release()
	customTime := time.Since(startTime)

	fmt.Printf("Encoded with custom allocator in %v\n", customTime)

	// Print memory stats after custom allocator
	runtime.ReadMemStats(&memStats)
	fmt.Printf("Heap allocations: %d bytes\n", memStats.HeapAlloc)

	// Step 5: Decoding with custom allocator
	fmt.Println("\nStep 5: Decoding with custom allocator")

	// Create decoder with custom allocator
	customDecoder := decode.NewDecoder(
		schema,
		decode.WithAllocator(customAllocator),
	)

	var decodedLogs []LogEntry
	startTime = time.Now()
	if err := customDecoder.Decode(customRecord, &decodedLogs); err != nil {
		log.Fatalf("Failed to decode with custom allocator: %v", err)
	}
	decodeTime := time.Since(startTime)

	fmt.Printf("Decoded %d log entries with custom allocator in %v\n",
		len(decodedLogs), decodeTime)

	// Step 6: Memory management best practices
	fmt.Println("\nStep 6: Memory management best practices")
	fmt.Println("1. Always defer record.Release() to free Arrow memory")
	fmt.Println("2. Use custom allocators for better memory control")
	fmt.Println("3. In zero-allocation mode, keep Arrow records valid while accessing decoded data")
	fmt.Println("4. For large datasets, process in batches to control memory usage")
	fmt.Println("5. Consider using memory.NewCheckedAllocator for debugging memory leaks")

	// Step 7: Demonstrate memory-efficient batch processing
	fmt.Println("\nStep 7: Memory-efficient batch processing")

	batchSize := 1000
	numBatches := len(logs) / batchSize

	fmt.Printf("Processing %d log entries in %d batches of %d\n",
		len(logs), numBatches, batchSize)

	for i := 0; i < numBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(logs) {
			end = len(logs)
		}

		batch := logs[start:end]

		// Process batch
		batchRecord, err := defaultEncoder.Encode(batch)
		if err != nil {
			log.Fatalf("Failed to encode batch %d: %v", i, err)
		}

		// Important: Release the record after processing each batch
		fmt.Printf("Processed batch %d/%d (%d entries)\n",
			i+1, numBatches, len(batch))
		batchRecord.Release()

		// Force garbage collection between batches (for demonstration only)
		if i%5 == 0 {
			runtime.GC()
		}
	}
}
