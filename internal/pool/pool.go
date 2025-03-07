package pool

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	maxBucketSize  = 32
	ringBufferSize = 64
)

// MemPool wraps Arrow's memory pool with additional pooling features.
type MemPool struct {
	pool        memory.Allocator
	bufferPools [maxBucketSize]sync.Pool
	ringBuffer  *ByteRingBuffer
}

// ByteRingBuffer is a fixed-size circular buffer for frequently used byte slices.
type ByteRingBuffer struct {
	buffer [][]byte
	head   int
	tail   int
	size   int
	mu     sync.Mutex
}

// NewByteRingBuffer creates a new ring buffer with the specified size.
func NewByteRingBuffer(size int) *ByteRingBuffer {
	return &ByteRingBuffer{
		buffer: make([][]byte, size),
		size:   size,
	}
}

// Put adds a slice to the ring buffer.
func (r *ByteRingBuffer) Put(slice []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.buffer[r.head] = slice
	r.head = (r.head + 1) % r.size
	if r.head == r.tail {
		r.tail = (r.tail + 1) % r.size
	}
}

// Get retrieves a slice from the ring buffer.
func (r *ByteRingBuffer) Get() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.head == r.tail {
		return nil
	}

	slice := r.buffer[r.tail]
	r.buffer[r.tail] = nil
	r.tail = (r.tail + 1) % r.size
	return slice
}

// ValueRingBuffer is a fixed-size circular buffer for frequently used interface{} slices.
type ValueRingBuffer struct {
	buffer [][]interface{}
	head   int
	tail   int
	size   int
	mu     sync.Mutex
}

// NewValueRingBuffer creates a new ring buffer with the specified size.
func NewValueRingBuffer(size int) *ValueRingBuffer {
	return &ValueRingBuffer{
		buffer: make([][]interface{}, size),
		size:   size,
	}
}

// Put adds a slice to the ring buffer.
func (r *ValueRingBuffer) Put(slice []interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.buffer[r.head] = slice
	r.head = (r.head + 1) % r.size
	if r.head == r.tail {
		r.tail = (r.tail + 1) % r.size
	}
}

// Get retrieves a slice from the ring buffer.
func (r *ValueRingBuffer) Get() []interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.head == r.tail {
		return nil
	}

	slice := r.buffer[r.tail]
	r.buffer[r.tail] = nil
	r.tail = (r.tail + 1) % r.size
	return slice
}

// NewMemPool creates a new memory pool with size-based buckets.
func NewMemPool() *MemPool {
	p := &MemPool{
		pool:       memory.NewGoAllocator(),
		ringBuffer: NewByteRingBuffer(ringBufferSize),
	}

	// Initialize size-based buckets
	for i := 0; i < maxBucketSize; i++ {
		size := 1 << i // Powers of 2
		p.bufferPools[i].New = func() interface{} {
			return make([]byte, 0, size)
		}
	}

	return p
}

// Allocator returns the underlying Arrow memory allocator.
func (p *MemPool) Allocator() memory.Allocator {
	return p.pool
}

// getBucketIndex returns the appropriate bucket index for the given size.
func getBucketIndex(size int) int {
	if size <= 0 {
		return 0
	}
	// Find the smallest power of 2 that fits the size
	index := 0
	for size > 1 {
		size >>= 1
		index++
	}
	if index >= maxBucketSize {
		return maxBucketSize - 1
	}
	return index
}

// GetBuffer returns a buffer of at least the specified size.
func (p *MemPool) GetBuffer(size int) []byte {
	// Try ring buffer first for common sizes
	if size <= 64 {
		if buf := p.ringBuffer.Get(); buf != nil {
			return buf[:0]
		}
	}

	// Get from size-based bucket
	index := getBucketIndex(size)
	if buf := p.bufferPools[index].Get(); buf != nil {
		return buf.([]byte)
	}

	// Create new buffer if none available
	return make([]byte, 0, 1<<index)
}

// PutBuffer returns a buffer to the pool.
func (p *MemPool) PutBuffer(buf []byte) {
	if cap(buf) <= 64 {
		// Store frequently used sizes in ring buffer
		p.ringBuffer.Put(buf[:0])
		return
	}

	// Return to size-based bucket
	index := getBucketIndex(cap(buf))
	if index < maxBucketSize {
		p.bufferPools[index].Put(buf[:0])
	}
}

// ValuePool manages pools of interface{} slices.
type ValuePool struct {
	pool       sync.Pool
	ringBuffer *ValueRingBuffer
}

// NewValuePool creates a new value pool.
func NewValuePool() *ValuePool {
	return &ValuePool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]interface{}, 0, 64)
			},
		},
		ringBuffer: NewValueRingBuffer(ringBufferSize),
	}
}

// Get returns a slice from the pool.
func (p *ValuePool) Get() []interface{} {
	// Try ring buffer first
	if slice := p.ringBuffer.Get(); slice != nil {
		return slice[:0]
	}
	return p.pool.Get().([]interface{})
}

// Put returns a slice to the pool.
func (p *ValuePool) Put(slice []interface{}) {
	// Clear references to help GC
	for i := range slice {
		slice[i] = nil
	}

	if cap(slice) <= 64 {
		// Store frequently used sizes in ring buffer
		p.ringBuffer.Put(slice[:0])
		return
	}

	p.pool.Put(slice[:0])
}

// SIMDPool manages pools of typed slices for SIMD operations
type SIMDPool struct {
	int8Pool    sync.Pool
	int16Pool   sync.Pool
	int32Pool   sync.Pool
	int64Pool   sync.Pool
	uint8Pool   sync.Pool
	uint16Pool  sync.Pool
	uint32Pool  sync.Pool
	uint64Pool  sync.Pool
	float32Pool sync.Pool
	float64Pool sync.Pool
	boolPool    sync.Pool
}

// NewSIMDPool creates a new SIMD pool
func NewSIMDPool() *SIMDPool {
	return &SIMDPool{
		int8Pool: sync.Pool{
			New: func() interface{} {
				return make([]int8, 0, 1024)
			},
		},
		int16Pool: sync.Pool{
			New: func() interface{} {
				return make([]int16, 0, 1024)
			},
		},
		int32Pool: sync.Pool{
			New: func() interface{} {
				return make([]int32, 0, 1024)
			},
		},
		int64Pool: sync.Pool{
			New: func() interface{} {
				return make([]int64, 0, 1024)
			},
		},
		uint8Pool: sync.Pool{
			New: func() interface{} {
				return make([]uint8, 0, 1024)
			},
		},
		uint16Pool: sync.Pool{
			New: func() interface{} {
				return make([]uint16, 0, 1024)
			},
		},
		uint32Pool: sync.Pool{
			New: func() interface{} {
				return make([]uint32, 0, 1024)
			},
		},
		uint64Pool: sync.Pool{
			New: func() interface{} {
				return make([]uint64, 0, 1024)
			},
		},
		float32Pool: sync.Pool{
			New: func() interface{} {
				return make([]float32, 0, 1024)
			},
		},
		float64Pool: sync.Pool{
			New: func() interface{} {
				return make([]float64, 0, 1024)
			},
		},
		boolPool: sync.Pool{
			New: func() interface{} {
				return make([]bool, 0, 1024)
			},
		},
	}
}

// GetInt8Slice returns a slice from the int8 pool
func (p *SIMDPool) GetInt8Slice(size int) []int8 {
	slice := p.int8Pool.Get().([]int8)
	if cap(slice) < size {
		return make([]int8, size)
	}
	return slice[:size]
}

// PutInt8Slice returns a slice to the int8 pool
func (p *SIMDPool) PutInt8Slice(slice []int8) {
	if cap(slice) <= 1024 {
		p.int8Pool.Put(slice[:0])
	}
}

// GetInt16Slice returns a slice from the int16 pool
func (p *SIMDPool) GetInt16Slice(size int) []int16 {
	slice := p.int16Pool.Get().([]int16)
	if cap(slice) < size {
		return make([]int16, size)
	}
	return slice[:size]
}

// PutInt16Slice returns a slice to the int16 pool
func (p *SIMDPool) PutInt16Slice(slice []int16) {
	if cap(slice) <= 1024 {
		p.int16Pool.Put(slice[:0])
	}
}

// GetBoolSlice returns a slice from the bool pool
func (p *SIMDPool) GetBoolSlice(size int) []bool {
	slice := p.boolPool.Get().([]bool)
	if cap(slice) < size {
		return make([]bool, size)
	}
	return slice[:size]
}

// PutBoolSlice returns a slice to the bool pool
func (p *SIMDPool) PutBoolSlice(slice []bool) {
	if cap(slice) <= 1024 {
		p.boolPool.Put(slice[:0])
	}
}

// ... Add similar methods for other types ...
