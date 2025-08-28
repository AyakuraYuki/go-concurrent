package futuretask

import (
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// PlanRun creates a future task with a runnable function.
func PlanRun(f func() error) *Task {
	return &Task{
		run: f,
	}
}

// PlanSupply creates a future task with a supplier function
func PlanSupply(f func() (any, error)) *Task {
	return &Task{
		get:        f,
		resultChan: make(chan any, 1),
	}
}

// Execute the given tasks, and return an error if one of the tasks results in an error.
func Execute(futures ...*Task) (err error) {
	if len(futures) == 0 {
		return nil
	}

	eg := new(errgroup.Group)

	for i := range futures {
		if futures[i] == nil {
			continue
		}
		eg.Go(futures[i].execute)
		atomic.AddInt64(&metrics.created, 1)
	}

	return eg.Wait()
}

// Run the given tasks, it will block until all the tasks are done.
// Because Run does not return errors, you should handle the error manually.
func Run(futures ...*Task) {
	if len(futures) == 0 {
		return
	}

	eg := new(errgroup.Group)

	for i := range futures {
		if futures[i] == nil {
			continue
		}
		eg.Go(futures[i].execute)
		atomic.AddInt64(&metrics.created, 1)
	}

	_ = eg.Wait()
}
