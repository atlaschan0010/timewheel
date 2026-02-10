package timewheel

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestNew 测试创建调度器
func TestNew(t *testing.T) {
	cfg := DefaultConfig()
	s := New(cfg)
	if s == nil {
		t.Fatal("New() returned nil")
	}
	if s.nearTerm == nil {
		t.Fatal("nearTerm is nil")
	}
	if s.farTerm == nil {
		t.Fatal("farTerm is nil")
	}
	if s.engine == nil {
		t.Fatal("engine is nil")
	}
}

// TestSchedule 测试调度延迟任务
func TestSchedule(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond // 加快测试
	s := New(cfg)
	s.Start()
	defer s.Stop()

	var executed atomic.Bool
	taskID := s.Schedule(50*time.Millisecond, func() {
		executed.Store(true)
	})

	if taskID == "" {
		t.Fatal("Schedule returned empty taskID")
	}

	time.Sleep(200 * time.Millisecond)

	if !executed.Load() {
		t.Error("Task was not executed")
	}
}

// TestSchedulePeriodic 测试周期任务
func TestSchedulePeriodic(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	var count atomic.Int32
	taskID := s.SchedulePeriodic(50*time.Millisecond, func() {
		count.Add(1)
	})

	if taskID == "" {
		t.Fatal("SchedulePeriodic returned empty taskID")
	}

	time.Sleep(280 * time.Millisecond)

	// 应该执行约 5 次 (50ms, 100ms, 150ms, 200ms, 250ms)
	c := count.Load()
	if c < 3 || c > 7 {
		t.Errorf("Periodic task executed %d times, expected 3-7", c)
	}
}

// TestCancel 测试取消任务
func TestCancel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	var executed atomic.Bool
	taskID := s.Schedule(100*time.Millisecond, func() {
		executed.Store(true)
	})

	// 取消任务
	if !s.Cancel(taskID) {
		t.Error("Cancel returned false for existing task")
	}

	time.Sleep(200 * time.Millisecond)

	if executed.Load() {
		t.Error("Cancelled task was executed")
	}
}

// TestCancelNonExistent 测试取消不存在的任务
func TestCancelNonExistent(t *testing.T) {
	cfg := DefaultConfig()
	s := New(cfg)

	if s.Cancel("non-existent-id") {
		t.Error("Cancel should return false for non-existent task")
	}
}

// TestCancelPeriodicTask 测试取消周期任务
func TestCancelPeriodicTask(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	var count atomic.Int32
	taskID := s.SchedulePeriodic(50*time.Millisecond, func() {
		count.Add(1)
	})

	// 等待执行 2~3 次
	time.Sleep(130 * time.Millisecond)

	// 取消周期任务
	s.Cancel(taskID)
	countAtCancel := count.Load()

	// 等待看是否还有执行
	time.Sleep(200 * time.Millisecond)
	countAfter := count.Load()

	// 取消后不应该再有新的执行（允许1次延迟）
	if countAfter > countAtCancel+1 {
		t.Errorf("Periodic task kept executing after cancel: at_cancel=%d, after=%d", countAtCancel, countAfter)
	}
}

// TestFarTermTask 测试远期任务迁移
func TestFarTermTask(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	cfg.NearTermThreshold = 100 * time.Millisecond // 100ms 为分界线
	s := New(cfg)
	s.Start()
	defer s.Stop()

	var executed atomic.Bool
	// 调度一个远期任务（超过 100ms）
	s.Schedule(150*time.Millisecond, func() {
		executed.Store(true)
	})

	// 验证任务确实进入了远期结构
	time.Sleep(10 * time.Millisecond)
	stats := s.Stats()
	if stats.FarTermTasks != 1 {
		t.Errorf("Expected 1 far-term task, got %d", stats.FarTermTasks)
	}

	// 等待任务执行完成
	time.Sleep(300 * time.Millisecond)

	if !executed.Load() {
		t.Error("Far-term task was not executed")
	}

	// 检查是否被迁移
	stats = s.Stats()
	if stats.TasksMigrated == 0 {
		t.Error("Task should have been migrated")
	}
}

// TestMultipleTasks 测试多个任务
func TestMultipleTasks(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	var count atomic.Int32

	// 调度多个不同延迟的任务
	for i := 0; i < 10; i++ {
		delay := time.Duration(50+i*20) * time.Millisecond
		s.Schedule(delay, func() {
			count.Add(1)
		})
	}

	time.Sleep(500 * time.Millisecond)

	c := count.Load()
	if c != 10 {
		t.Errorf("Expected 10 tasks executed, got %d", c)
	}
}

// TestStats 测试统计信息
func TestStats(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	// 调度一些任务
	s.Schedule(50*time.Millisecond, func() {})
	s.Schedule(100*time.Millisecond, func() {})

	time.Sleep(200 * time.Millisecond)

	stats := s.Stats()
	if stats.TasksExecuted != 2 {
		t.Errorf("TasksExecuted = %d, want 2", stats.TasksExecuted)
	}
}

// TestScheduleWithIDReplaces 测试相同 ID 会替换旧任务
func TestScheduleWithIDReplaces(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	var oldExecuted atomic.Bool
	var newExecuted atomic.Bool

	// 用固定 ID 调度
	s.ScheduleWithID("same-id", 200*time.Millisecond, func() {
		oldExecuted.Store(true)
	})

	// 用相同 ID 替换
	time.Sleep(20 * time.Millisecond)
	s.ScheduleWithID("same-id", 100*time.Millisecond, func() {
		newExecuted.Store(true)
	})

	time.Sleep(300 * time.Millisecond)

	if oldExecuted.Load() {
		t.Error("Old task should not have been executed after replacement")
	}
	if !newExecuted.Load() {
		t.Error("New task should have been executed")
	}
}

// TestZeroDelay 测试零延迟任务
func TestZeroDelay(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	var executed atomic.Bool
	s.Schedule(0, func() {
		executed.Store(true)
	})

	time.Sleep(100 * time.Millisecond)

	if !executed.Load() {
		t.Error("Zero-delay task was not executed")
	}
}

// TestSparseWheel 测试稀疏时间轮
func TestSparseWheel(t *testing.T) {
	sw := NewSparseWheel()

	task1 := &Task{ID: "task1", ExpireAt: 1000}
	task2 := &Task{ID: "task2", ExpireAt: 1000}
	task3 := &Task{ID: "task3", ExpireAt: 2000}

	// 插入任务
	sw.Insert(1, task1)
	sw.Insert(1, task2)
	sw.Insert(2, task3)

	if sw.Len() != 3 {
		t.Errorf("Len = %d, want 3", sw.Len())
	}

	// 获取任务
	tasks := sw.Get(1)
	if len(tasks) != 2 {
		t.Errorf("Get(1) returned %d tasks, want 2", len(tasks))
	}

	// 弹出任务
	popped := sw.PopTick(1)
	if len(popped) != 2 {
		t.Errorf("PopTick(1) returned %d tasks, want 2", len(popped))
	}

	if sw.Len() != 1 {
		t.Errorf("Len after pop = %d, want 1", sw.Len())
	}

	// 删除任务（现在是 O(1)）
	if !sw.Delete("task3") {
		t.Error("Delete(task3) should return true")
	}

	if sw.Len() != 0 {
		t.Errorf("Len after delete = %d, want 0", sw.Len())
	}
}

// TestSparseWheelDeleteO1 测试稀疏时间轮 O(1) 删除
func TestSparseWheelDeleteO1(t *testing.T) {
	sw := NewSparseWheel()

	// 插入大量任务到不同 tick
	for i := 0; i < 1000; i++ {
		task := &Task{ID: "task_" + string(rune(i)), ExpireAt: int64(i * 100)}
		sw.Insert(int64(i), task)
	}

	if sw.Len() != 1000 {
		t.Errorf("Len = %d, want 1000", sw.Len())
	}

	// O(1) 删除（通过反向索引直接定位 tick）
	if !sw.Delete("task_" + string(rune(500))) {
		t.Error("Delete should return true")
	}

	if sw.Len() != 999 {
		t.Errorf("Len after delete = %d, want 999", sw.Len())
	}
}

// TestSparseWheelPopTicksUpTo 测试批量弹出
func TestSparseWheelPopTicksUpTo(t *testing.T) {
	sw := NewSparseWheel()

	// 插入任务到 tick 1, 3, 5, 7, 9
	for i := int64(1); i <= 9; i += 2 {
		task := &Task{ID: "task_" + string(rune(i)), ExpireAt: i * 100}
		sw.Insert(i, task)
	}

	if sw.Len() != 5 {
		t.Errorf("Len = %d, want 5", sw.Len())
	}

	// 弹出 tick <= 5 的所有任务
	tasks := sw.PopTicksUpTo(5)
	if len(tasks) != 3 {
		t.Errorf("PopTicksUpTo(5) returned %d tasks, want 3", len(tasks))
	}

	if sw.Len() != 2 {
		t.Errorf("Len after pop = %d, want 2", sw.Len())
	}
}

// TestSparseWheelPeekNextTick 测试 O(1) 查找最小 tick
func TestSparseWheelPeekNextTick(t *testing.T) {
	sw := NewSparseWheel()

	_, exists := sw.PeekNextTick()
	if exists {
		t.Error("PeekNextTick should return false for empty wheel")
	}

	sw.Insert(5, &Task{ID: "task5", ExpireAt: 500})
	sw.Insert(3, &Task{ID: "task3", ExpireAt: 300})
	sw.Insert(7, &Task{ID: "task7", ExpireAt: 700})

	tick, exists := sw.PeekNextTick()
	if !exists || tick != 3 {
		t.Errorf("PeekNextTick = (%d, %v), want (3, true)", tick, exists)
	}

	// 弹出 tick 3
	sw.PopTick(3)
	tick, exists = sw.PeekNextTick()
	if !exists || tick != 5 {
		t.Errorf("PeekNextTick after pop = (%d, %v), want (5, true)", tick, exists)
	}
}

// TestSparseWheelDuplicateID 测试相同 ID 插入到不同 tick
func TestSparseWheelDuplicateID(t *testing.T) {
	sw := NewSparseWheel()

	task1 := &Task{ID: "task1", ExpireAt: 100}
	task2 := &Task{ID: "task1", ExpireAt: 200} // 相同 ID，不同 tick

	sw.Insert(1, task1)
	sw.Insert(2, task2) // 应该移除 tick 1 中的旧任务

	if sw.Len() != 1 {
		t.Errorf("Len = %d, want 1 (duplicate ID should replace)", sw.Len())
	}

	// 旧的 tick 1 应该为空
	tasks := sw.PopTick(1)
	if len(tasks) != 0 {
		t.Errorf("Old tick should be empty, got %d tasks", len(tasks))
	}

	// 新的 tick 2 应该有任务
	tasks = sw.PopTick(2)
	if len(tasks) != 1 {
		t.Errorf("New tick should have 1 task, got %d", len(tasks))
	}
}

// TestSkipList 测试跳表
func TestSkipList(t *testing.T) {
	sl := NewSkipList()

	tasks := []*Task{
		{ID: "task1", ExpireAt: 300},
		{ID: "task2", ExpireAt: 100},
		{ID: "task3", ExpireAt: 200},
	}

	// 插入任务
	for _, task := range tasks {
		sl.Insert(task)
	}

	if sl.Len() != 3 {
		t.Errorf("Len = %d, want 3", sl.Len())
	}

	// 查看最小元素
	min := sl.PeekMin()
	if min == nil || min.ID != "task2" {
		t.Errorf("PeekMin = %v, want task2", min)
	}

	// 弹出最小元素
	popped := sl.PopMin()
	if popped == nil || popped.ID != "task2" {
		t.Errorf("PopMin = %v, want task2", popped)
	}

	if sl.Len() != 2 {
		t.Errorf("Len after pop = %d, want 2", sl.Len())
	}

	// 按 ID 删除
	deleted := sl.DeleteByID("task3")
	if deleted == nil || deleted.ID != "task3" {
		t.Errorf("DeleteByID = %v, want task3", deleted)
	}

	if sl.Len() != 1 {
		t.Errorf("Len after delete = %d, want 1", sl.Len())
	}
}

// TestSkipListDuplicateInsert 测试跳表重复插入（更新）
func TestSkipListDuplicateInsert(t *testing.T) {
	sl := NewSkipList()

	task := &Task{ID: "task1", ExpireAt: 100}
	sl.Insert(task)

	// 更新任务
	task2 := &Task{ID: "task1", ExpireAt: 200}
	sl.Insert(task2)

	if sl.Len() != 1 {
		t.Errorf("Len = %d, want 1 (after update)", sl.Len())
	}

	min := sl.PeekMin()
	if min.ExpireAt != 200 {
		t.Errorf("ExpireAt = %d, want 200 (after update)", min.ExpireAt)
	}
}

// TestTicklessWake 测试 Tickless 引擎确实被使用
func TestTicklessWake(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	start := time.Now()

	var executed atomic.Bool
	s.Schedule(50*time.Millisecond, func() {
		executed.Store(true)
	})

	// 等待执行
	time.Sleep(150 * time.Millisecond)

	elapsed := time.Since(start)
	if !executed.Load() {
		t.Error("Task was not executed")
	}

	// Tickless 设计下，任务应该在 ~50ms 执行，不应该等很久
	if elapsed > 300*time.Millisecond {
		t.Errorf("Tickless engine took too long: %v", elapsed)
	}
}

// BenchmarkSchedule 基准测试调度
func BenchmarkSchedule(b *testing.B) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Schedule(100*time.Millisecond, func() {})
	}
}

// BenchmarkCancel 基准测试取消
func BenchmarkCancel(b *testing.B) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	// 预先创建任务
	taskIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		taskIDs[i] = s.Schedule(100*time.Second, func() {})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Cancel(taskIDs[i])
	}
}

// BenchmarkScheduleAndCancel 基准测试调度+取消
func BenchmarkScheduleAndCancel(b *testing.B) {
	cfg := DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	s := New(cfg)
	s.Start()
	defer s.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := s.Schedule(100*time.Millisecond, func() {})
		s.Cancel(id)
	}
}
