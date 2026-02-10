package timewheel

import (
	"math/rand/v2"
	"sync"
)

const (
	maxLevel    = 32
	probability = 0.25
)

// SkipListNode 跳表节点
type SkipListNode struct {
	task    *Task
	forward []*SkipListNode
	level   int
}

// SkipList 跳表实现（按 ExpireAt 排序）
type SkipList struct {
	head    *SkipListNode
	level   int
	length  int
	taskMap map[string]*SkipListNode // 用于 O(1) 按 ID 查找
	mu      sync.RWMutex
}

// NewSkipList 创建新的跳表
func NewSkipList() *SkipList {
	return &SkipList{
		head:    &SkipListNode{forward: make([]*SkipListNode, maxLevel)},
		level:   1,
		taskMap: make(map[string]*SkipListNode),
	}
}

// randomLevel 随机生成节点层级
func (sl *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && rand.Float64() < probability {
		level++
	}
	return level
}

// Insert 插入任务
func (sl *SkipList) Insert(task *Task) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// 如果任务已存在，先删除
	if node, exists := sl.taskMap[task.ID]; exists {
		sl.deleteNode(node)
	}

	update := make([]*SkipListNode, maxLevel)
	node := sl.head

	// 查找插入位置（按 ExpireAt 排序，相同 ExpireAt 按 ID 排序）
	for i := sl.level - 1; i >= 0; i-- {
		for node.forward[i] != nil {
			cmp := compareTasks(node.forward[i].task, task)
			if cmp < 0 {
				node = node.forward[i]
			} else {
				break
			}
		}
		update[i] = node
	}

	// 生成新节点的层级
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	// 创建新节点
	newNode := &SkipListNode{
		task:    task,
		forward: make([]*SkipListNode, newLevel),
		level:   newLevel,
	}

	// 更新指针
	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.taskMap[task.ID] = newNode
	sl.length++
}

// compareTasks 比较两个任务（按 ExpireAt，相同则按 ID）
func compareTasks(a, b *Task) int {
	if a.ExpireAt < b.ExpireAt {
		return -1
	}
	if a.ExpireAt > b.ExpireAt {
		return 1
	}
	// ExpireAt 相同，按 ID 排序
	if a.ID < b.ID {
		return -1
	}
	if a.ID > b.ID {
		return 1
	}
	return 0
}

// deleteNode 删除指定节点（内部方法，调用者需持有锁）
func (sl *SkipList) deleteNode(target *SkipListNode) {
	update := make([]*SkipListNode, maxLevel)
	node := sl.head

	// 使用排序特性进行精确搜索，确保正确找到前驱节点
	for i := sl.level - 1; i >= 0; i-- {
		for node.forward[i] != nil {
			if node.forward[i] == target {
				break
			}
			cmp := compareTasks(node.forward[i].task, target.task)
			if cmp < 0 {
				node = node.forward[i]
			} else {
				break
			}
		}
		update[i] = node
	}

	// 更新指针
	for i := 0; i < target.level; i++ {
		if update[i].forward[i] == target {
			update[i].forward[i] = target.forward[i]
		}
	}

	// 更新层级
	for sl.level > 1 && sl.head.forward[sl.level-1] == nil {
		sl.level--
	}

	delete(sl.taskMap, target.task.ID)
	sl.length--
}

// DeleteByID 根据任务 ID 删除任务
func (sl *SkipList) DeleteByID(taskID string) *Task {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	node, exists := sl.taskMap[taskID]
	if !exists {
		return nil
	}

	task := node.task
	sl.deleteNode(node)
	return task
}

// PeekMin 查看最小元素（不删除）
func (sl *SkipList) PeekMin() *Task {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	if sl.head.forward[0] == nil {
		return nil
	}
	return sl.head.forward[0].task
}

// PopMin 弹出最小元素
func (sl *SkipList) PopMin() *Task {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if sl.head.forward[0] == nil {
		return nil
	}

	node := sl.head.forward[0]
	sl.deleteNode(node)
	return node.task
}

// Len 返回元素数量
func (sl *SkipList) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.length
}
