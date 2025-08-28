package concurrent

import (
	"sync"
	"sync/atomic"
)

// Run creates a Task with given runnable function, then executes the
// function immediately in goroutine.
func Run(run func() error) *Task[any] {
	task := &Task[any]{
		run: run,
	}
	task.state.Store(int32(StatePending))
	task.cond = sync.NewCond(&task.mu)

	atomic.AddInt64(&metrics.Created, 1)

	go task.execute()

	return task
}

// Get creates a Task with given supplier function, then executes the
// function immediately in goroutine.
// After task done, the result will be cached into task.result.
func Get[T any](get func() (T, error)) *Task[T] {
	task := &Task[T]{
		get: get,
	}
	task.state.Store(int32(StatePending))
	task.cond = sync.NewCond(&task.mu)

	atomic.AddInt64(&metrics.Created, 1)

	go task.execute()

	return task
}

// WaitAll tasks finish
func WaitAll[T any](tasks ...*Task[T]) {
	if len(tasks) == 0 {
		return
	}
	for i := range tasks {
		tasks[i].Wait()
	}
}
