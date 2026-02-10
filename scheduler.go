package timewheel

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Task 定时任务
type Task struct {
	ID        string
	ExpireAt  int64       // 到期时间戳（毫秒）
	Interval  int64       // 周期任务间隔（毫秒），0 表示非周期
	Cancelled atomic.Bool // 取消标记（惰性删除）
	Fn        func()      // 任务函数
}

// IsPeriodic 判断是否为周期任务
func (t *Task) IsPeriodic() bool {
	return t.Interval > 0
}

// Scheduler 自适应混合定时调度器
type Scheduler struct {
	// 配置参数
	tickInterval       int64 // tick 间隔（毫秒）
	nearTermThreshold  int64 // 近期/远期分界线（毫秒）
	migrationBatchSize int   // 每次迁移的最大任务数
	lazyDeleteEnabled  bool  // 是否启用惰性删除

	// 核心组件
	nearTerm *SparseWheel    // 稀疏时间轮（近期任务）
	farTerm  *SkipList       // 跳表（远期任务）
	engine   *TicklessEngine // 无空转唤醒引擎

	// 控制
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running atomic.Bool

	// 统计
	tasksExecuted  atomic.Uint64
	tasksCancelled atomic.Uint64
	tasksMigrated  atomic.Uint64
}

// Config 调度器配置
type Config struct {
	TickInterval       time.Duration // 近期轮精度，默认 1ms
	NearTermThreshold  time.Duration // 近期/远期分界线，默认 60s
	MigrationBatchSize int           // 每次迁移的最大任务数，默认 128
	LazyDeleteEnabled  bool          // 是否启用惰性删除，默认 true
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		TickInterval:       1 * time.Millisecond,
		NearTermThreshold:  60 * time.Second,
		MigrationBatchSize: 128,
		LazyDeleteEnabled:  true,
	}
}

// New 创建新的调度器
func New(cfg Config) *Scheduler {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 1 * time.Millisecond
	}
	if cfg.NearTermThreshold <= 0 {
		cfg.NearTermThreshold = 60 * time.Second
	}
	if cfg.MigrationBatchSize <= 0 {
		cfg.MigrationBatchSize = 128
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Scheduler{
		tickInterval:       cfg.TickInterval.Milliseconds(),
		nearTermThreshold:  cfg.NearTermThreshold.Milliseconds(),
		migrationBatchSize: cfg.MigrationBatchSize,
		lazyDeleteEnabled:  cfg.LazyDeleteEnabled,
		ctx:                ctx,
		cancel:             cancel,
	}

	s.nearTerm = NewSparseWheel()
	s.farTerm = NewSkipList()
	s.engine = NewTicklessEngine(s)

	return s
}

// Start 启动调度器
func (s *Scheduler) Start() {
	if s.running.CompareAndSwap(false, true) {
		s.wg.Add(1)
		go s.run()
	}
}

// Stop 停止调度器
func (s *Scheduler) Stop() {
	if s.running.CompareAndSwap(true, false) {
		s.cancel()
		s.engine.Wake() // 唤醒引擎使其退出
		s.wg.Wait()
	}
}

// Schedule 调度一个延迟任务
func (s *Scheduler) Schedule(delay time.Duration, fn func()) string {
	return s.ScheduleWithID(generateID(), delay, fn)
}

// ScheduleWithID 使用指定 ID 调度延迟任务
// 如果 ID 已存在，旧任务会被取消并替换为新任务
func (s *Scheduler) ScheduleWithID(id string, delay time.Duration, fn func()) string {
	if delay < 0 {
		delay = 0
	}

	// 取消可能存在的同 ID 旧任务
	s.cancelInternal(id)

	now := time.Now().UnixMilli()
	expireAt := now + delay.Milliseconds()

	task := &Task{
		ID:       id,
		ExpireAt: expireAt,
		Fn:       fn,
	}

	s.insertTask(task)
	return id
}

// SchedulePeriodic 调度周期任务
func (s *Scheduler) SchedulePeriodic(interval time.Duration, fn func()) string {
	return s.SchedulePeriodicWithID(generateID(), interval, fn)
}

// SchedulePeriodicWithID 使用指定 ID 调度周期任务
func (s *Scheduler) SchedulePeriodicWithID(id string, interval time.Duration, fn func()) string {
	if interval <= 0 {
		interval = 1 * time.Millisecond
	}

	// 取消可能存在的同 ID 旧任务
	s.cancelInternal(id)

	now := time.Now().UnixMilli()
	expireAt := now + interval.Milliseconds()

	task := &Task{
		ID:       id,
		ExpireAt: expireAt,
		Interval: interval.Milliseconds(),
		Fn:       fn,
	}

	s.insertTask(task)
	return id
}

// Cancel 取消任务
func (s *Scheduler) Cancel(taskID string) bool {
	if s.cancelInternal(taskID) {
		s.tasksCancelled.Add(1)
		return true
	}
	return false
}

// cancelInternal 内部取消方法（不更新统计）
func (s *Scheduler) cancelInternal(taskID string) bool {
	// 先在近期轮中查找（O(1)）
	if s.nearTerm.Delete(taskID) {
		return true
	}

	// 在远期结构中查找（O(log n)）
	if task := s.farTerm.DeleteByID(taskID); task != nil {
		return true
	}

	return false
}

// insertTask 插入任务到合适的存储结构
func (s *Scheduler) insertTask(task *Task) {
	now := time.Now().UnixMilli()
	delay := task.ExpireAt - now

	if delay < s.nearTermThreshold {
		// 近期任务 -> 稀疏时间轮
		tick := task.ExpireAt / s.tickInterval
		s.nearTerm.Insert(tick, task)
	} else {
		// 远期任务 -> 跳表
		s.farTerm.Insert(task)
	}

	// 通知引擎检查是否需要提前唤醒
	s.engine.NotifyIfEarlier(task.ExpireAt)
}

// run 主调度循环 — 真正的 Tickless 设计
func (s *Scheduler) run() {
	defer s.wg.Done()

	for {
		// 检查是否已停止
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// 1. 计算下一次唤醒时间
		nextWake := s.engine.CalculateNextWakeTime()

		if nextWake == 0 {
			// 无任务，阻塞等待新任务插入
			if !s.engine.WaitForTask() {
				return // context 取消
			}
			continue
		}

		// 2. 睡眠到下一个到期时间（或被新任务唤醒）
		s.engine.SleepUntil(nextWake)

		// 3. 推进时间，处理到期任务
		s.advance()
	}
}

// advance 时间推进
func (s *Scheduler) advance() {
	now := time.Now().UnixMilli()
	currentTick := now / s.tickInterval

	// 1. 触发所有到期任务（包括可能漏掉的 tick）
	tasks := s.nearTerm.PopTicksUpTo(currentTick)
	for _, task := range tasks {
		if task.Cancelled.Load() {
			continue
		}

		// 执行任务
		go s.executeTask(task)
	}

	// 2. 惰性迁移：拉取即将到期的远期任务
	s.migrateFarTermTasks(now)
}

// migrateFarTermTasks 将即将到期的远期任务迁移到近期轮
func (s *Scheduler) migrateFarTermTasks(now int64) {
	migrated := 0
	threshold := now + s.nearTermThreshold

	for migrated < s.migrationBatchSize {
		task := s.farTerm.PeekMin()
		if task == nil {
			break
		}

		// 检查是否需要迁移
		if task.ExpireAt > threshold {
			break // 还不需要迁移
		}

		// 从远期结构移除
		s.farTerm.PopMin()

		// 如果已取消，跳过
		if task.Cancelled.Load() {
			continue
		}

		// 迁移到近期轮
		taskTick := task.ExpireAt / s.tickInterval
		s.nearTerm.Insert(taskTick, task)
		migrated++
		s.tasksMigrated.Add(1)
	}
}

// executeTask 执行任务
func (s *Scheduler) executeTask(task *Task) {
	defer func() {
		if r := recover(); r != nil {
			// 捕获 panic，避免单个任务崩溃影响调度器
		}
	}()

	task.Fn()
	s.tasksExecuted.Add(1)

	// 处理周期任务：创建新的 Task 实例避免并发修改
	if task.IsPeriodic() && !task.Cancelled.Load() {
		now := time.Now().UnixMilli()
		newTask := &Task{
			ID:       task.ID,
			ExpireAt: now + task.Interval,
			Interval: task.Interval,
			Fn:       task.Fn,
		}
		s.insertTask(newTask)
	}
}

// Stats 返回统计信息
func (s *Scheduler) Stats() Stats {
	return Stats{
		TasksExecuted:  s.tasksExecuted.Load(),
		TasksCancelled: s.tasksCancelled.Load(),
		TasksMigrated:  s.tasksMigrated.Load(),
		NearTermTasks:  s.nearTerm.Len(),
		FarTermTasks:   s.farTerm.Len(),
	}
}

// Stats 统计信息
type Stats struct {
	TasksExecuted  uint64
	TasksCancelled uint64
	TasksMigrated  uint64
	NearTermTasks  int
	FarTermTasks   int
}

// generateID 生成唯一 ID
var idCounter atomic.Uint64

func generateID() string {
	return fmt.Sprintf("task_%d_%d", time.Now().UnixNano(), idCounter.Add(1))
}
