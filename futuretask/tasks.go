package futuretask

import (
	"errors"
	"sync"
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

	var (
		wg sync.WaitGroup
		mu sync.RWMutex

		doneChan = make(chan bool)
	)

	for _, future := range futures {
		wg.Add(1)

		go func(future *Task, wg *sync.WaitGroup, mu *sync.RWMutex) {
			future.execute(wg)
			if future.err != nil {
				mu.Lock()
				err = errors.Join(err, future.err)
				mu.Unlock()
			}
		}(future, &wg, &mu)
	}

	go func() {
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()

		close(doneChan)
	}()

	<-doneChan

	return err
}

// Run the given tasks, it will block until all the tasks are done.
// Because Run does not return errors, you should handle the error manually.
func Run(futures ...*Task) {
	if len(futures) == 0 {
		return
	}

	var (
		wg sync.WaitGroup

		doneChan = make(chan bool)
	)

	for _, future := range futures {
		wg.Add(1)

		go func(future *Task, wg *sync.WaitGroup) {
			future.execute(wg)
		}(future, &wg)
	}

	go func() {
		wg.Wait()
		close(doneChan)
	}()

	<-doneChan
}
