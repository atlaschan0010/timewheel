package timewheel

import (
	"container/heap"
	"sync"
)

// SparseWheel 稀疏时间轮
type SparseWheel struct {
	// map[tick]map[taskID]*Task
	buckets map[int64]map[string]*Task
	// 反向索引: taskID -> tick（实现 O(1) 取消）
	taskIndex map[string]int64
	// 最小堆：追踪最早的 tick（避免 O(n) 遍历）
	tickHeap *tickMinHeap
	// tick 集合：用于去重堆中的 tick
	tickSet map[int64]struct{}
	mu      sync.RWMutex
}

// NewSparseWheel 创建新的稀疏时间轮
func NewSparseWheel() *SparseWheel {
	h := &tickMinHeap{}
	heap.Init(h)
	return &SparseWheel{
		buckets:   make(map[int64]map[string]*Task),
		taskIndex: make(map[string]int64),
		tickHeap:  h,
		tickSet:   make(map[int64]struct{}),
	}
}

// Insert 插入任务到指定 tick
func (sw *SparseWheel) Insert(tick int64, task *Task) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// 如果任务已存在（相同 ID），先移除旧的
	if oldTick, exists := sw.taskIndex[task.ID]; exists {
		if oldBucket, ok := sw.buckets[oldTick]; ok {
			delete(oldBucket, task.ID)
			if len(oldBucket) == 0 {
				delete(sw.buckets, oldTick)
				sw.removeTickFromHeap(oldTick)
			}
		}
	}

	bucket, exists := sw.buckets[tick]
	if !exists {
		bucket = make(map[string]*Task)
		sw.buckets[tick] = bucket
		// 新 tick，加入最小堆
		if _, inSet := sw.tickSet[tick]; !inSet {
			heap.Push(sw.tickHeap, tick)
			sw.tickSet[tick] = struct{}{}
		}
	}
	bucket[task.ID] = task
	sw.taskIndex[task.ID] = tick
}

// Delete 从时间轮中删除任务 — O(1) 通过反向索引
func (sw *SparseWheel) Delete(taskID string) bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	tick, exists := sw.taskIndex[taskID]
	if !exists {
		return false
	}

	bucket, ok := sw.buckets[tick]
	if !ok {
		// 索引不一致，清理
		delete(sw.taskIndex, taskID)
		return false
	}

	if task, taskExists := bucket[taskID]; taskExists {
		task.Cancelled.Store(true)
		delete(bucket, taskID)
		delete(sw.taskIndex, taskID)

		// 如果 bucket 为空，删除 bucket 和堆中的 tick
		if len(bucket) == 0 {
			delete(sw.buckets, tick)
			sw.removeTickFromHeap(tick)
		}
		return true
	}

	delete(sw.taskIndex, taskID)
	return false
}

// Get 获取指定 tick 的 bucket（不删除）
func (sw *SparseWheel) Get(tick int64) map[string]*Task {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	bucket, exists := sw.buckets[tick]
	if !exists {
		return nil
	}

	// 返回副本避免并发问题
	result := make(map[string]*Task, len(bucket))
	for k, v := range bucket {
		result[k] = v
	}
	return result
}

// PopTick 弹出指定 tick 的所有任务并从时间轮移除
func (sw *SparseWheel) PopTick(tick int64) []*Task {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	bucket, exists := sw.buckets[tick]
	if !exists {
		return nil
	}

	// 提取所有任务
	tasks := make([]*Task, 0, len(bucket))
	for _, task := range bucket {
		tasks = append(tasks, task)
		delete(sw.taskIndex, task.ID)
	}

	// 删除 bucket
	delete(sw.buckets, tick)
	sw.removeTickFromHeap(tick)

	return tasks
}

// PopTicksUpTo 弹出所有 <= maxTick 的任务（处理漏掉的 tick）
func (sw *SparseWheel) PopTicksUpTo(maxTick int64) []*Task {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	var allTasks []*Task

	// 从最小堆中持续弹出 <= maxTick 的 tick
	for sw.tickHeap.Len() > 0 {
		minTick := (*sw.tickHeap)[0]
		if minTick > maxTick {
			break
		}

		heap.Pop(sw.tickHeap)
		delete(sw.tickSet, minTick)

		bucket, exists := sw.buckets[minTick]
		if !exists {
			continue
		}

		for _, task := range bucket {
			allTasks = append(allTasks, task)
			delete(sw.taskIndex, task.ID)
		}
		delete(sw.buckets, minTick)
	}

	return allTasks
}

// PeekNextTick 查看下一个有任务的 tick — O(1) 通过最小堆
// 返回 tick 和是否存在
func (sw *SparseWheel) PeekNextTick() (int64, bool) {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	// 清理堆顶可能已经被删除的 tick
	for sw.tickHeap.Len() > 0 {
		minTick := (*sw.tickHeap)[0]
		if _, exists := sw.buckets[minTick]; exists {
			return minTick, true
		}
		// 堆顶 tick 已无 bucket，弹出
		heap.Pop(sw.tickHeap)
		delete(sw.tickSet, minTick)
	}
	return 0, false
}

// Len 返回当前任务数量
func (sw *SparseWheel) Len() int {
	sw.mu.RLock()
	defer sw.mu.RUnlock()
	return len(sw.taskIndex)
}

// BucketsCount 返回 bucket 数量
func (sw *SparseWheel) BucketsCount() int {
	sw.mu.RLock()
	defer sw.mu.RUnlock()
	return len(sw.buckets)
}

// removeTickFromHeap 从堆中标记移除某个 tick（惰性删除）
func (sw *SparseWheel) removeTickFromHeap(tick int64) {
	delete(sw.tickSet, tick)
	// 堆中的 tick 会在 PeekNextTick 中惰性清理
}

// tickMinHeap 最小堆实现
type tickMinHeap []int64

func (h tickMinHeap) Len() int           { return len(h) }
func (h tickMinHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h tickMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *tickMinHeap) Push(x interface{}) {
	*h = append(*h, x.(int64))
}

func (h *tickMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
