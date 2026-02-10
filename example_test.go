package timewheel_test

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/atlaschan0010/timewheel"
)

func ExampleScheduler() {
	// 创建调度器
	cfg := timewheel.DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond // 加快示例速度
	scheduler := timewheel.New(cfg)

	// 启动调度器
	scheduler.Start()
	defer scheduler.Stop()

	// 示例 1: 延迟任务
	done := make(chan struct{})
	scheduler.Schedule(100*time.Millisecond, func() {
		fmt.Println("Delayed task executed!")
		close(done)
	})

	<-done

	// Output:
	// Delayed task executed!
}

func ExampleScheduler_periodic() {
	cfg := timewheel.DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	scheduler := timewheel.New(cfg)
	scheduler.Start()
	defer scheduler.Stop()

	var count atomic.Int32
	tickerID := scheduler.SchedulePeriodic(50*time.Millisecond, func() {
		c := count.Add(1)
		fmt.Printf("Tick %d\n", c)
	})

	time.Sleep(180 * time.Millisecond)
	scheduler.Cancel(tickerID)

	fmt.Println("Ticker stopped")

	// Output:
	// Tick 1
	// Tick 2
	// Tick 3
	// Ticker stopped
}

func ExampleScheduler_cancel() {
	cfg := timewheel.DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	scheduler := timewheel.New(cfg)
	scheduler.Start()
	defer scheduler.Stop()

	executed := false
	taskID := scheduler.Schedule(100*time.Millisecond, func() {
		executed = true
		fmt.Println("This should not print!")
	})

	// 取消任务
	if scheduler.Cancel(taskID) {
		fmt.Println("Task cancelled successfully")
	}

	time.Sleep(200 * time.Millisecond)

	if !executed {
		fmt.Println("Task was not executed (as expected)")
	}

	// Output:
	// Task cancelled successfully
	// Task was not executed (as expected)
}

func ExampleScheduler_stats() {
	cfg := timewheel.DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	scheduler := timewheel.New(cfg)
	scheduler.Start()
	defer scheduler.Stop()

	// 调度多个任务
	for i := 0; i < 5; i++ {
		scheduler.Schedule(50*time.Millisecond, func() {})
	}

	// 调度一个远期任务（超过 60s 阈值，默认在远期结构）
	scheduler.Schedule(70*time.Second, func() {})

	time.Sleep(150 * time.Millisecond)

	stats := scheduler.Stats()
	fmt.Printf("Executed: %d\n", stats.TasksExecuted)
	fmt.Printf("Near-term tasks: %d\n", stats.NearTermTasks)
	fmt.Printf("Far-term tasks: %d\n", stats.FarTermTasks)

	// Output:
	// Executed: 5
	// Near-term tasks: 0
	// Far-term tasks: 1
}

func ExampleScheduler_withID() {
	cfg := timewheel.DefaultConfig()
	cfg.TickInterval = 10 * time.Millisecond
	scheduler := timewheel.New(cfg)
	scheduler.Start()
	defer scheduler.Stop()

	done := make(chan struct{})

	// 使用自定义 ID
	scheduler.ScheduleWithID("my-task-1", 200*time.Millisecond, func() {
		fmt.Println("This should NOT print (replaced)")
	})

	// 更新同名任务（会替换旧任务）
	scheduler.ScheduleWithID("my-task-1", 100*time.Millisecond, func() {
		fmt.Println("Task 1 (updated) executed!")
		close(done)
	})

	<-done

	// Output:
	// Task 1 (updated) executed!
}
