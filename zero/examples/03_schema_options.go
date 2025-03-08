package examples

import (
	"fmt"
	"log"
	"time"

	"github.com/TFMV/arrowgen/zero/encode"
	"github.com/TFMV/arrowgen/zero/schema"
	"github.com/apache/arrow-go/v18/arrow"
)

// Event represents a struct with various field types for schema options demonstration
type Event struct {
	ID          int64              `arrow:"id"`
	Name        string             `arrow:"name"`
	Description string             `arrow:"description,omitempty"`
	Timestamp   time.Time          `arrow:"timestamp"`
	Tags        []string           `arrow:"tags,omitempty"`
	Metrics     map[string]float64 `arrow:"metrics,omitempty"`
	IsActive    bool               `arrow:"is_active"`
}

// SchemaOptions demonstrates various schema inference options and customizations
func SchemaOptions() {
	// Step 1: Create sample data
	fmt.Println("Step 1: Creating sample data")
	events := []Event{
		{
			ID:        1,
			Name:      "Server Start",
			Timestamp: time.Now().Add(-1 * time.Hour),
			IsActive:  true,
		},
		{
			ID:          2,
			Name:        "Database Backup",
			Description: "Weekly backup completed successfully",
			Timestamp:   time.Now().Add(-30 * time.Minute),
			Tags:        []string{"database", "backup", "weekly"},
			Metrics: map[string]float64{
				"duration_seconds": 120.5,
				"size_mb":          1024.0,
			},
			IsActive: true,
		},
	}
	fmt.Printf("Created %d events\n", len(events))

	// Step 2: Basic schema inference
	fmt.Println("\nStep 2: Basic schema inference")
	basicSchema, err := schema.SchemaFromStruct(Event{})
	if err != nil {
		log.Fatalf("Failed to infer basic schema: %v", err)
	}
	fmt.Printf("Basic schema has %d fields\n", len(basicSchema.Fields()))

	// Print the basic schema fields
	fmt.Println("\nBasic schema fields:")
	for i, field := range basicSchema.Fields() {
		fmt.Printf("  Field %d: %s (Type: %s, Nullable: %v)\n",
			i+1, field.Name, field.Type, field.Nullable)
	}

	// Step 3: Schema with custom options
	fmt.Println("\nStep 3: Schema with custom options")

	// Create schema options
	opts := &schema.SchemaOptions{
		// Set maximum nesting depth for complex types
		MaxNestingDepth: 3,

		// Disable dictionary encoding for string fields
		EnableDictionary: false,
	}

	// Infer schema with options
	customSchema, err := schema.SchemaFromStructWithOptions(Event{}, opts)
	if err != nil {
		log.Fatalf("Failed to infer schema with options: %v", err)
	}

	fmt.Printf("Custom schema has %d fields\n", len(customSchema.Fields()))

	// Print the custom schema fields
	fmt.Println("\nCustom schema fields:")
	for i, field := range customSchema.Fields() {
		fmt.Printf("  Field %d: %s (Type: %s, Nullable: %v)\n",
			i+1, field.Name, field.Type, field.Nullable)
	}

	// Step 4: Encode data with custom schema
	fmt.Println("\nStep 4: Encoding data with custom schema")
	encoder := encode.NewEncoder(customSchema)
	record, err := encoder.Encode(events)
	if err != nil {
		log.Fatalf("Failed to encode data: %v", err)
	}
	defer record.Release()

	fmt.Printf("Encoded %d rows with custom schema\n", record.NumRows())

	// Step 5: Demonstrate schema metadata
	fmt.Println("\nStep 5: Schema with metadata")

	// Create metadata
	metadata := arrow.NewMetadata(
		[]string{"created_by", "version", "environment"},
		[]string{"arrowgen", "1.0.0", "production"},
	)

	// Create schema with metadata
	metadataSchema := arrow.NewSchema(customSchema.Fields(), &metadata)

	fmt.Println("Schema metadata:")
	for i := 0; i < metadata.Len(); i++ {
		key, value := metadata.Keys()[i], metadata.Values()[i]
		fmt.Printf("  %s: %s\n", key, value)
	}

	// Step 6: Encode with schema containing metadata
	fmt.Println("\nStep 6: Encoding with schema containing metadata")
	metadataEncoder := encode.NewEncoder(metadataSchema)
	metadataRecord, err := metadataEncoder.Encode(events)
	if err != nil {
		log.Fatalf("Failed to encode data with metadata schema: %v", err)
	}
	defer metadataRecord.Release()

	fmt.Printf("Encoded %d rows with metadata schema\n", metadataRecord.NumRows())

	// Verify metadata is preserved
	recordMetadata := metadataRecord.Schema().Metadata()
	fmt.Println("\nRecord schema metadata:")
	for i := 0; i < recordMetadata.Len(); i++ {
		key, value := recordMetadata.Keys()[i], recordMetadata.Values()[i]
		fmt.Printf("  %s: %s\n", key, value)
	}
}
