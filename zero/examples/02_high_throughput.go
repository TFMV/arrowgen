package examples

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/TFMV/arrowgen/zero/encode"
	"github.com/TFMV/arrowgen/zero/schema"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Product represents a more complex struct for high-throughput processing
type Product struct {
	ID          int64     `arrow:"id"`
	Name        string    `arrow:"name"`
	Description string    `arrow:"description"`
	Price       float64   `arrow:"price"`
	InStock     bool      `arrow:"in_stock"`
	Categories  []string  `arrow:"categories"`
	CreatedAt   time.Time `arrow:"created_at"`
	UpdatedAt   time.Time `arrow:"updated_at"`
}

// generateLargeProductDataset creates a large dataset for demonstrating high-throughput processing
func generateLargeProductDataset(count int) []Product {
	products := make([]Product, count)
	now := time.Now()

	for i := 0; i < count; i++ {
		products[i] = Product{
			ID:          int64(i + 1),
			Name:        fmt.Sprintf("Product %d", i+1),
			Description: fmt.Sprintf("This is a detailed description for product %d with lots of text to simulate a real-world scenario", i+1),
			Price:       float64(i*10) + 9.99,
			InStock:     i%5 != 0, // 80% of products are in stock
			Categories:  []string{"Electronics", "Gadgets", fmt.Sprintf("Category-%d", i%10)},
			CreatedAt:   now.Add(-time.Duration(i) * time.Hour),
			UpdatedAt:   now,
		}
	}

	return products
}

// HighThroughputMode demonstrates the high-throughput mode of the Zero API
// which is optimized for maximum processing speed with parallel execution
func HighThroughputMode() {
	// Generate a large dataset (10,000 products)
	fmt.Println("Step 1: Generating large dataset (10,000 products)")
	products := generateLargeProductDataset(10000)
	fmt.Printf("Generated %d products\n", len(products))

	// Infer schema from struct
	fmt.Println("\nStep 2: Inferring schema from Product struct")
	schema, err := schema.SchemaFromStruct(Product{})
	if err != nil {
		log.Fatalf("Failed to infer schema: %v", err)
	}
	fmt.Printf("Inferred schema with %d fields\n", len(schema.Fields()))

	// Create encoder with high-throughput mode
	fmt.Println("\nStep 3: Creating encoder with high-throughput mode")
	encoder := encode.NewEncoder(
		schema,
		encode.WithMode(encode.ModeHighThroughput),
		encode.WithAllocator(memory.NewGoAllocator()),
		encode.WithWorkers(runtime.GOMAXPROCS(0)), // Use all available CPU cores
	)

	// Measure encoding time
	fmt.Println("\nStep 4: Encoding data with high-throughput mode")
	startTime := time.Now()
	record, err := encoder.Encode(products)
	if err != nil {
		log.Fatalf("Failed to encode data: %v", err)
	}
	defer record.Release()
	encodingTime := time.Since(startTime)

	fmt.Printf("Encoded %d rows with %d columns in %v\n",
		record.NumRows(), record.NumCols(), encodingTime)
	fmt.Printf("Throughput: %.2f records/second\n",
		float64(len(products))/encodingTime.Seconds())

	// Compare with zero-allocation mode
	fmt.Println("\nStep 5: Comparing with zero-allocation mode")
	zeroAllocEncoder := encode.NewEncoder(schema) // Default is zero-allocation mode

	startTime = time.Now()
	zeroAllocRecord, err := zeroAllocEncoder.Encode(products)
	if err != nil {
		log.Fatalf("Failed to encode data with zero-allocation mode: %v", err)
	}
	defer zeroAllocRecord.Release()
	zeroAllocTime := time.Since(startTime)

	fmt.Printf("Zero-allocation mode encoded %d rows in %v\n",
		zeroAllocRecord.NumRows(), zeroAllocTime)
	fmt.Printf("Throughput: %.2f records/second\n",
		float64(len(products))/zeroAllocTime.Seconds())

	// Calculate speedup
	speedup := zeroAllocTime.Seconds() / encodingTime.Seconds()
	fmt.Printf("\nHigh-throughput mode is %.2fx faster than zero-allocation mode\n", speedup)

	fmt.Println("\nKey benefits of high-throughput mode:")
	fmt.Println("1. Parallel processing with worker pools")
	fmt.Println("2. Pre-allocated buffer pools")
	fmt.Println("3. Optimized for batch processing")
	fmt.Println("4. Scales with available CPU cores")
}
