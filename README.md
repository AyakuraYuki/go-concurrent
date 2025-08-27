# go-concurrent

A lightweight Go library for executing concurrent tasks with future-like functionality, providing both immediate execution and planned execution patterns.

## Features

- **Two execution models**: Immediate execution `concurrent` and planned execution `futuretask`.
- **Type-safe results**: Generic support for strongly-typed return values.
- **Error handling**: Built-in error propagation and panic recovery.
- **Task states**: Track task lifecycle in `concurrent` (Pending, Running, Done, Failed).
- **Metrics**: Built-in task execution metrics.
- **Flexible waiting**: Wait for individual tasks or multiple tasks together.

## Installation

```bash
go get -u github.com/AyakuraYuki/go-concurrent
```

## Quick Start

### Package `concurrent` - Immediate execution

The `concurrent` package executes tasks immediately when created:

```go
package main

import (
	"fmt"
	"time"

	"github.com/AyakuraYuki/go-concurrent/concurrent"
)

func main() {
	// Simply run a task
	task1 := concurrent.Run(func() error {
		time.Sleep(1 * time.Second)
		fmt.Println("Task 1 completed")
		return nil
	})

	// Get a result from a task
	task2 := concurrent.Get(func() (string, error) {
		time.Sleep(500 * time.Millisecond)
		return "hello, world", nil
	})

	// Wait for tasks to complete
	task1.Wait()
	result, err := task2.Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("Result:", result)
}

```

### Package `futuretask` - Planned execution

The `futuretask` package allows you to plan tasks and execute them together:

```go
package main

import (
	"fmt"
	"time"

	"github.com/AyakuraYuki/go-concurrent/futuretask"
)

func main() {
	// Plan tasks (not executed yet)
	task1 := futuretask.PlanRun(func() error {
		time.Sleep(1 * time.Second)
		fmt.Println("Planned task completed")
		return nil
	})

	task2 := futuretask.PlanSupply(func() (any, error) {
		time.Sleep(500 * time.Millisecond)
		return "Future result", nil
	})

	// Execute all tasks together
	if err := futuretask.Execute(task1, task2); err != nil {
		panic(err)
	}

	// Get results
	result := task2.Get()
	fmt.Println("Future result:", result)
}

```

## API Reference

### Package `concurrent`

#### Creating Tasks

- `concurrent.Run(func() error) *Task[any]` - Execute a function immediately, returns no value
- `concurrent.Get[T](func() (T, error)) *Task[T]` - Execute a function immediately, returns typed value

#### Task Methods

- `task.Result() (T, error)` - Get result and error (blocks until done)
- `task.Get() T` - Get result only, ignore error (blocks until done)
- `task.Err() error` - Get error only (blocks until done)
- `task.Wait()` - Wait for task completion
- `task.State() State` - Get current task state
- `task.IsRunning() bool` - Check if task is currently running
- `task.IsDone() bool` - Check if task is completed
- `task.SpendTime() time.Duration` - Get execution time

#### Utility Functions

- `concurrent.WaitAll[T](tasks ...*Task[T])` - Wait for multiple tasks to complete
- `concurrent.MetricsStatus()` - Get global task metrics
- `concurrent.ResetMetricsStatus()` - Reset metrics counters

#### Task States

- StatePending - task is pending
- StateRunning - task is running
- StateDone - task is completed without error
- StateFailed - task is failed
- StateCancelled - task is cancelled

### Package `futuretask`

#### Planning Tasks

- `futuretask.PlanRun(func() error) *Task` - Plan a runnable task
- `futuretask.PlanSupply(func() (any, error)) *Task` - Plan a supplier task

#### Execution Functions

- `futuretask.Execute(tasks ...*Task) error` - Execute tasks and return first error encountered
- `futuretask.Run(tasks ...*Task)` - Execute all tasks, blocks until all tasks done

#### Task Methods

- `task.Result() (any, error)` - Get result and error
- `task.Get() any` - Get result only
- `task.Err() error` - Get error only

## Examples

### Multiple Concurrent Tasks

```go
package example

import (
	"fmt"
	"time"

	"github.com/AyakuraYuki/go-concurrent/concurrent"
)

func RunMultipleTasks() {
	// Create multiple tasks
	tasks := make([]*concurrent.Task[int], 10)
	for i := 0; i < 10; i++ {
		index := i
		tasks[i] = concurrent.Get(func() (int, error) {
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			return index * 2, nil
		})
	}

	// Wait for all tasks
	concurrent.WaitAll(tasks...)

	// Collect results
	for i, task := range tasks {
		result := task.Get()
		fmt.Printf("Task %d result: %d\n", i, result)
	}
}

```

### Error Handling

```go
package main

import (
	"errors"
	"fmt"

	"github.com/AyakuraYuki/go-concurrent/concurrent"
)

func HandleErrors() {
	task := concurrent.Get(func() (string, error) {
		return "", errors.New("something went wrong")
	})

	result, err := task.Result()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Result:", result)
}

```

### Task Monitoring

```go
package main

import (
	"fmt"
	"time"

	"github.com/AyakuraYuki/go-concurrent/concurrent"
)

func MonitorTask() {
	task := concurrent.Run(func() error {
		time.Sleep(2 * time.Second)
		return nil
	})

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for !task.IsDone() {
		<-ticker.C
		fmt.Printf("work (ut: %s)\n", task.SpendTime())
	}

	fmt.Printf("task state: %v (ut: %v)\n", task.State(), task.SpendTime())
}

```

### Planned vs Immediate Execution

```go
package main

import (
	"github.com/AyakuraYuki/go-concurrent/concurrent"
	"github.com/AyakuraYuki/go-concurrent/futuretask"
)

func main() {
	// Immediate execution - starts running immediately
	_ = concurrent.Get(func() (string, error) {
		return "immediate", nil
	})

	// Planned execution - waits for Execute() call
	plannedTask := futuretask.PlanSupply(func() (any, error) {
		return "planned", nil
	})
	// Execute the planned task
	_ = futuretask.Execute(plannedTask)
}

```

## Performance Considerations

- Tasks are executed in separate goroutines.
- Results are cached after first retrieval.
- The library includes panic recovery for robust error handling.
- Metrics tracking has minimal overhead.
- Use `WaitAll()` for efficient bulk waiting.

## Metrics

The `concurrent` package provides built-in metrics:

```text
metrics := concurrent.MetricsStatus()
fmt.Printf("Created: %d, Done: %d, Failed: %d, Cancelled: %d\n",
    metrics.Created, metrics.Done, metrics.Failed, metrics.Cancelled)
```

## Thread Safety

- All task operations are thread-safe.
- Multiple goroutines can safely call `Result()`, `Get()`, and other methods.
- Results are cached and returned consistently across calls.

## Error Handling

- Panics are automatically recovered and converted to errors.
- Errors are preserved and returned through `Result()` and `Err()` methods.
- The `futuretask.Execute()` function fails fast on first error.
- The `futuretask.Run()` function continues execution and allows manual error handling.

## License

MIT License
