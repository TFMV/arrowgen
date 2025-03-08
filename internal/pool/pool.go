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
		p.bufferPools[index].Put(&buf)
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
				slice := make([]interface{}, 0, 64)
				return &slice
			},
		},
		ringBuffer: NewValueRingBuffer(32),
	}
}

// Get returns a slice from the pool.
func (p *ValuePool) Get() []interface{} {
	// Try ring buffer first
	if slice := p.ringBuffer.Get(); slice != nil {
		return slice[:0]
	}

	if v := p.pool.Get(); v != nil {
		pslice := v.(*[]interface{})
		return (*pslice)[:0]
	}

	return make([]interface{}, 0, 64)
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

	// Store larger slices in sync.Pool
	slice = slice[:0]
	p.pool.Put(&slice)
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
	stringPool  sync.Pool
	bytesPool   sync.Pool
}

// NewSIMDPool creates a new SIMD pool
func NewSIMDPool() *SIMDPool {
	return &SIMDPool{
		int8Pool: sync.Pool{
			New: func() interface{} {
				slice := make([]int8, 0, 1024)
				return &slice
			},
		},
		int16Pool: sync.Pool{
			New: func() interface{} {
				slice := make([]int16, 0, 1024)
				return &slice
			},
		},
		int32Pool: sync.Pool{
			New: func() interface{} {
				slice := make([]int32, 0, 1024)
				return &slice
			},
		},
		int64Pool: sync.Pool{
			New: func() interface{} {
				slice := make([]int64, 0, 1024)
				return &slice
			},
		},
		uint8Pool: sync.Pool{
			New: func() interface{} {
				slice := make([]uint8, 0, 1024)
				return &slice
			},
		},
		uint16Pool: sync.Pool{
			New: func() interface{} {
				slice := make([]uint16, 0, 1024)
				return &slice
			},
		},
		uint32Pool: sync.Pool{
			New: func() interface{} {
				slice := make([]uint32, 0, 1024)
				return &slice
			},
		},
		uint64Pool: sync.Pool{
			New: func() interface{} {
				slice := make([]uint64, 0, 1024)
				return &slice
			},
		},
		float32Pool: sync.Pool{
			New: func() interface{} {
				slice := make([]float32, 0, 1024)
				return &slice
			},
		},
		float64Pool: sync.Pool{
			New: func() interface{} {
				slice := make([]float64, 0, 1024)
				return &slice
			},
		},
		boolPool: sync.Pool{
			New: func() interface{} {
				slice := make([]bool, 0, 1024)
				return &slice
			},
		},
		stringPool: sync.Pool{
			New: func() interface{} {
				slice := make([]string, 0, 1024)
				return &slice
			},
		},
		bytesPool: sync.Pool{
			New: func() interface{} {
				slice := make([]byte, 0, 1024)
				return &slice
			},
		},
	}
}

// GetInt8Slice returns a slice from the int8 pool
func (p *SIMDPool) GetInt8Slice(size int) []int8 {
	var slice []int8
	if v := p.int8Pool.Get(); v != nil {
		pslice := v.(*[]int8)
		slice = *pslice
	} else {
		slice = make([]int8, 0, size)
	}
	if cap(slice) < size {
		slice = make([]int8, size)
	} else {
		slice = slice[:size]
	}
	return slice
}

// PutInt8Slice returns a slice to the int8 pool
func (p *SIMDPool) PutInt8Slice(slice []int8) {
	if cap(slice) > 0 {
		slice = slice[:0]
		p.int8Pool.Put(&slice)
	}
}

// GetInt16Slice returns a slice from the int16 pool
func (p *SIMDPool) GetInt16Slice(size int) []int16 {
	var slice []int16
	if v := p.int16Pool.Get(); v != nil {
		pslice := v.(*[]int16)
		slice = *pslice
	} else {
		slice = make([]int16, 0, size)
	}
	if cap(slice) < size {
		slice = make([]int16, size)
	} else {
		slice = slice[:size]
	}
	return slice
}

// PutInt16Slice returns a slice to the int16 pool
func (p *SIMDPool) PutInt16Slice(slice []int16) {
	if cap(slice) > 0 {
		slice = slice[:0]
		p.int16Pool.Put(&slice)
	}
}

// GetBoolSlice returns a slice from the bool pool
func (p *SIMDPool) GetBoolSlice(size int) []bool {
	var slice []bool
	if v := p.boolPool.Get(); v != nil {
		pslice := v.(*[]bool)
		slice = *pslice
	} else {
		slice = make([]bool, 0, size)
	}
	if cap(slice) < size {
		slice = make([]bool, size)
	} else {
		slice = slice[:size]
	}
	return slice
}

// PutBoolSlice returns a bool slice to the pool
func (p *SIMDPool) PutBoolSlice(slice []bool) {
	if cap(slice) > 0 {
		slice = slice[:0]
		p.boolPool.Put(&slice)
	}
}

// GetInt32Slice gets an int32 slice from the pool
func (p *SIMDPool) GetInt32Slice(size int) []int32 {
	var slice []int32
	if v := p.int32Pool.Get(); v != nil {
		pslice := v.(*[]int32)
		slice = *pslice
	} else {
		slice = make([]int32, 0, size)
	}
	if cap(slice) < size {
		slice = make([]int32, size)
	} else {
		slice = slice[:size]
	}
	return slice
}

// PutInt32Slice returns an int32 slice to the pool
func (p *SIMDPool) PutInt32Slice(slice []int32) {
	if cap(slice) > 0 {
		slice = slice[:0]
		p.int32Pool.Put(&slice)
	}
}

// GetInt64Slice gets an int64 slice from the pool
func (p *SIMDPool) GetInt64Slice(size int) []int64 {
	var slice []int64
	if v := p.int64Pool.Get(); v != nil {
		pslice := v.(*[]int64)
		slice = *pslice
	} else {
		slice = make([]int64, 0, size)
	}
	if cap(slice) < size {
		slice = make([]int64, size)
	} else {
		slice = slice[:size]
	}
	return slice
}

// PutInt64Slice returns an int64 slice to the pool
func (p *SIMDPool) PutInt64Slice(slice []int64) {
	if cap(slice) > 0 {
		slice = slice[:0]
		p.int64Pool.Put(&slice)
	}
}

// GetStringSlice gets a string slice from the pool
func (p *SIMDPool) GetStringSlice(size int) []string {
	var slice []string
	if v := p.stringPool.Get(); v != nil {
		pslice := v.(*[]string)
		slice = *pslice
	} else {
		slice = make([]string, 0, size)
	}
	if cap(slice) < size {
		slice = make([]string, size)
	} else {
		slice = slice[:size]
	}
	return slice
}

// PutStringSlice returns a string slice to the pool
func (p *SIMDPool) PutStringSlice(slice []string) {
	if cap(slice) > 0 {
		slice = slice[:0]
		p.stringPool.Put(&slice)
	}
}

// GetBytesSlice gets a slice of byte slices from the pool
func (p *SIMDPool) GetBytesSlice(size int) [][]byte {
	// For byte slices, we need a slice of slices
	return make([][]byte, size)
}

// PutBytesSlice returns a slice of byte slices to the pool
func (p *SIMDPool) PutBytesSlice(slice [][]byte) {
	// No-op as we don't pool these due to variable sizes
}

// GetFloat32Slice returns a slice from the float32 pool
func (p *SIMDPool) GetFloat32Slice(size int) []float32 {
	var slice []float32
	if v := p.float32Pool.Get(); v != nil {
		pslice := v.(*[]float32)
		slice = *pslice
	} else {
		slice = make([]float32, 0, size)
	}
	if cap(slice) < size {
		slice = make([]float32, size)
	} else {
		slice = slice[:size]
	}
	return slice
}

// PutFloat32Slice returns a float32 slice to the pool
func (p *SIMDPool) PutFloat32Slice(slice []float32) {
	if cap(slice) > 0 {
		slice = slice[:0]
		p.float32Pool.Put(&slice)
	}
}

// GetFloat64Slice returns a slice from the float64 pool
func (p *SIMDPool) GetFloat64Slice(size int) []float64 {
	var slice []float64
	if v := p.float64Pool.Get(); v != nil {
		pslice := v.(*[]float64)
		slice = *pslice
	} else {
		slice = make([]float64, 0, size)
	}
	if cap(slice) < size {
		slice = make([]float64, size)
	} else {
		slice = slice[:size]
	}
	return slice
}
