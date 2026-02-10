package timewheel

import (
	"sync"
	"sync/atomic"
	"time"
)

// TicklessEngine 无空转唤醒引擎
type TicklessEngine struct {
	scheduler *Scheduler

	// 唤醒信号
	wakeChan chan struct{}

	// 最早到期时间
	earliestExpire atomic.Int64 // 毫秒时间戳，0 表示无任务

	mu sync.Mutex
}

// NewTicklessEngine 创建新的无空转唤醒引擎
func NewTicklessEngine(scheduler *Scheduler) *TicklessEngine {
	return &TicklessEngine{
		scheduler: scheduler,
		wakeChan:  make(chan struct{}, 1),
	}
}

// NotifyIfEarlier 如果新的到期时间早于当前最早时间，唤醒引擎
func (te *TicklessEngine) NotifyIfEarlier(expireAt int64) {
	for {
		currentEarliest := te.earliestExpire.Load()

		// 如果当前无任务或新的到期时间更早
		if currentEarliest == 0 || expireAt < currentEarliest {
			if te.earliestExpire.CompareAndSwap(currentEarliest, expireAt) {
				te.Wake()
				return
			}
			// CAS 失败，重试
			continue
		}
		// 新的到期时间不更早，不需要唤醒
		return
	}
}

// Wake 唤醒引擎
func (te *TicklessEngine) Wake() {
	select {
	case te.wakeChan <- struct{}{}:
	default:
		// 通道已满，说明已经有唤醒信号了
	}
}

// SleepUntil 睡眠直到指定时间（或直到被唤醒或 context 取消）
// 返回 true 表示正常超时，false 表示被提前唤醒或取消
func (te *TicklessEngine) SleepUntil(targetTime int64) bool {
	if targetTime <= 0 {
		return true
	}

	now := time.Now().UnixMilli()
	sleepDuration := targetTime - now

	if sleepDuration <= 0 {
		return true // 已经到期
	}

	timer := time.NewTimer(time.Duration(sleepDuration) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true // 正常超时
	case <-te.wakeChan:
		return false // 被唤醒
	case <-te.scheduler.ctx.Done():
		return false // context 取消
	}
}

// WaitForTask 等待直到有任务可执行（无任务时阻塞）
// 返回 false 表示 context 已取消
func (te *TicklessEngine) WaitForTask() bool {
	select {
	case <-te.wakeChan:
		return true
	case <-te.scheduler.ctx.Done():
		return false
	}
}

// CalculateNextWakeTime 计算下一次唤醒时间
// 返回毫秒时间戳，0 表示无任务
func (te *TicklessEngine) CalculateNextWakeTime() int64 {
	var nextWake int64 = 0

	// 1. 检查近期轮最早到期的 tick
	if tick, exists := te.scheduler.nearTerm.PeekNextTick(); exists {
		nextWake = tick * te.scheduler.tickInterval
	}

	// 2. 检查远期结构最早到期的任务
	if task := te.scheduler.farTerm.PeekMin(); task != nil {
		if nextWake == 0 || task.ExpireAt < nextWake {
			nextWake = task.ExpireAt
		}
	}

	// 更新最早到期时间
	te.earliestExpire.Store(nextWake)

	return nextWake
}

// ResetEarliestExpire 重置最早到期时间
// 在任务被执行或取消后调用
func (te *TicklessEngine) ResetEarliestExpire() {
	te.earliestExpire.Store(0)
}
