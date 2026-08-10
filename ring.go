package zlog

import (
	"runtime"
	"sync/atomic"

	"github.com/petermattis/goid"
)

// 自旋退让阈值，可根据业务调参，日志场景128~1024均可
const spinYieldThreshold = 128

// sequence occupies one cache line.
type sequence struct {
	v atomic.Uint64

	// 8 + 56 = 64 bytes
	_ [56]byte // cache line padding
}

type cell[T any] struct {
	seq atomic.Uint64
	val T
}

// MPSCRing MPSC  RingBuffer
type MPSCRing[T any] struct {
	size uint64
	mask uint64

	// producer hotspot
	head sequence

	// consumer hotspot
	tail sequence

	buffer []cell[T]
}

// NewMPSCRing .
func NewMPSCRing[T any](size uint64) *MPSCRing[T] {
	if size == 0 {
		size = 1024
	}
	size = nextPowerOfTwo(size)

	r := &MPSCRing[T]{
		size:   size,
		mask:   size - 1,
		buffer: make([]cell[T], size),
	}

	for i := uint64(0); i < size; i++ {
		r.buffer[i].seq.Store(i)
	}

	return r
}

// Cap 缓存容量
func (r *MPSCRing[T]) Cap() uint64 {
	return r.size
}

// TryPublish return false if full
func (r *MPSCRing[T]) TryPublish(v T) bool {
	var spinCnt uint32
	for {
		pos := r.head.v.Load()
		slot := &r.buffer[pos&r.mask]
		seq := slot.seq.Load()
		diff := int64(seq) - int64(pos)
		if diff < 0 {
			return false // queue full
		}

		if diff == 0 {
			if r.head.v.CompareAndSwap(pos, pos+1) {
				slot.val = v
				slot.seq.Store(pos + 1)
				return true
			}
		}

		// CAS失败，自旋计数递增
		spinCnt++
		if spinCnt >= spinYieldThreshold {
			spinCnt = 0       // 重置计数，下次重新累积自旋
			runtime.Gosched() // 让出P，降低CPU空转
		}
	}
}

// TryRead Single Consumer Only
func (r *MPSCRing[T]) TryRead() (T, bool) {
	var zero T
	pos := r.tail.v.Load()
	slot := &r.buffer[pos&r.mask]
	if slot.seq.Load() != pos+1 {
		return zero, false
	}

	v := slot.val
	slot.seq.Store(pos + r.size)
	r.tail.v.Store(pos + 1)
	return v, true
}

// BatchRead Single Consumer Only
func (r *MPSCRing[T]) BatchRead(max uint64, dst []T) []T {
	tailStart := r.tail.v.Load()
	currentTail := tailStart

	for len(dst) < int(max) {
		pos := currentTail
		slot := &r.buffer[pos&r.mask]
		if slot.seq.Load() != pos+1 {
			break
		}

		dst = append(dst, slot.val)
		slot.seq.Store(pos + r.size)
		currentTail++
	}

	if currentTail != tailStart {
		r.tail.v.Store(currentTail)
	}

	return dst
}

// BatchDrop 批量删除元素
func (r *MPSCRing[T]) BatchDrop(max uint64) int {
	tailStart := r.tail.v.Load()
	currentTail := tailStart

	droped := 0
	for idx := range int(max) {
		pos := currentTail
		slot := &r.buffer[pos&r.mask]
		if slot.seq.Load() != pos+1 {
			droped = idx
			break
		}

		slot.seq.Store(pos + r.size)
		currentTail++
	}

	if currentTail != tailStart {
		r.tail.v.Store(currentTail)
	}

	return droped
}

// ShardedRing 多分片RingBuffer
type ShardedRing[T any] struct {
	shards         []*MPSCRing[T]
	mask           uint64
	numShards      uint64
	consumerCursor uint64
}

// NewShardedRing numShards为0时取核数
func NewShardedRing[T any](numShards, ringSize uint64) *ShardedRing[T] {
	if numShards <= 0 {
		numShards = uint64(runtime.GOMAXPROCS(0))
	}
	numShards = nextPowerOfTwo(numShards)

	s := &ShardedRing[T]{
		shards:    make([]*MPSCRing[T], numShards),
		mask:      numShards - 1,
		numShards: numShards,
	}

	for i := range numShards {
		s.shards[i] = NewMPSCRing[T](ringSize)
	}

	return s
}

// Cap 多分片缓存总容量
func (r *ShardedRing[T]) Cap() uint64 {
	if len(r.shards) == 0 {
		return 0
	}

	return uint64(len(r.shards)) *
		r.shards[0].Cap()
}

// PublishG 向当前G的shard写一条记录
func (r *ShardedRing[T]) PublishG(v T) bool {
	idx := uint64(goid.Get()) & r.mask
	return r.shards[idx].TryPublish(v)
}

// ReadG 从当前G的shard读取一条记录
func (r *ShardedRing[T]) ReadG() (T, bool) {
	idx := uint64(goid.Get()) & r.mask
	return r.shards[idx].TryRead()
}

// BatchDropG 从当前G的shard最多删除max条记录
func (r *ShardedRing[T]) BatchDropG(max uint64) int {
	idx := uint64(goid.Get()) & r.mask
	return r.shards[idx].BatchDrop(max)
}

// BatchRead 从一个shard最多读取max条记录
func (r *ShardedRing[T]) BatchRead(max uint64, dst []T) []T {
	idx := r.consumerCursor & r.mask
	r.consumerCursor++

	return r.shards[idx].BatchRead(max, dst)
}

func nextPowerOfTwo(v uint64) uint64 {
	if v <= 1 {
		return 1
	}

	v--

	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32

	return v + 1
}
