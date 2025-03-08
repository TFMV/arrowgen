package examples

import (
	"fmt"
	"os"
	"strconv"
)

// Main function to run the examples
func Main() {
	// Print available examples
	fmt.Println("Zero API Examples")
	fmt.Println("================")
	fmt.Println("1. Basic Usage (Schema Inference and Zero-Allocation Mode)")
	fmt.Println("2. High-Throughput Mode")
	fmt.Println("3. Schema Options")
	fmt.Println("4. Memory Management")
	fmt.Println("0. Run All Examples")
	fmt.Println()

	// Get example number from command line argument or prompt user
	var exampleNum int
	var err error

	if len(os.Args) > 1 {
		exampleNum, err = strconv.Atoi(os.Args[1])
		if err != nil {
			fmt.Printf("Invalid example number: %s\n", os.Args[1])
			os.Exit(1)
		}
	} else {
		fmt.Print("Enter example number (0-5): ")
		var input string
		fmt.Scanln(&input)
		exampleNum, err = strconv.Atoi(input)
		if err != nil {
			fmt.Printf("Invalid example number: %s\n", input)
			os.Exit(1)
		}
	}

	// Run the selected example
	switch exampleNum {
	case 0:
		runAllExamples()
	case 1:
		fmt.Println("\nRunning Example 1: Basic Usage")
		fmt.Println("==============================")
		BasicUsage()
	case 2:
		fmt.Println("\nRunning Example 2: High-Throughput Mode")
		fmt.Println("=====================================")
		HighThroughputMode()
	case 3:
		fmt.Println("\nRunning Example 3: Schema Options")
		fmt.Println("===============================")
		SchemaOptions()
	case 4:
		fmt.Println("\nRunning Example 4: Memory Management")
		fmt.Println("==================================")
		MemoryManagement()
	default:
		fmt.Printf("Invalid example number: %d\n", exampleNum)
		os.Exit(1)
	}
}

// Run all examples sequentially
func runAllExamples() {
	fmt.Println("\nRunning All Examples")
	fmt.Println("===================")

	fmt.Println("\n\nExample 1: Basic Usage")
	fmt.Println("=====================")
	BasicUsage()

	fmt.Println("\n\nExample 2: High-Throughput Mode")
	fmt.Println("==============================")
	HighThroughputMode()

	fmt.Println("\n\nExample 3: Schema Options")
	fmt.Println("\n\nExample 4: Schema Options")
	fmt.Println("=======================")
	SchemaOptions()

	fmt.Println("\n\nExample 5: Memory Management")
	fmt.Println("==========================")
	MemoryManagement()
}
