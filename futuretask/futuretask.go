package futuretask

import (
	"errors"
	"fmt"
	"sync"
)

// PlanRun creates a future task with a runnable function.
func PlanRun(f func() error) *Task {
	return &Task{
		r: &runnable{run: f},
	}
}

// PlanSupply creates a future task with a supplier function
func PlanSupply(f func() (any, error)) *Task {
	return &Task{
		s:          &supplier{get: f},
		resultChan: make(chan any, 1),
	}
}

// Execute the given tasks, and return an error if one of the tasks results in an error.
func Execute(futures ...*Task) error {
	if len(futures) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(futures))

	doneChan := make(chan bool)
	errChan := make(chan error)

	for _, future := range futures {
		go func(future *Task, wg *sync.WaitGroup, errChan chan error) {
			future.execute(wg)
			if future.err != nil {
				errChan <- future.err
			}
		}(future, &wg, errChan)
	}

	go func() {
		wg.Wait()
		close(doneChan)
		close(errChan)
	}()

	select {
	case err := <-errChan:
		return err
	case <-doneChan:
		return nil
	}
}

// Run the given tasks, it will block until all the tasks are done.
// Because Run does not return errors, you should handle the error manually.
func Run(futures ...*Task) {
	if len(futures) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(futures))

	doneChan := make(chan bool)

	for _, future := range futures {
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

// Task defines a unit of future tasks and allow the running of supplier/runnable function.
type Task struct {
	s          *supplier // supplier future
	resultChan chan any  // result channel
	result     any       // result holder

	r *runnable // runnable future

	err error // error holder
}

// Result returns both result and error from Task, it will block until the task is done.
func (task *Task) Result() (any, error) {
	result := task.Get()
	err := task.Err()
	return result, err
}

// Get the result from Task and ignore the error, it will block until the task is done.
// Note that a runnable Task has no result.
func (task *Task) Get() any {
	var empty any

	if task.s == nil {
		return empty // only supplier will return result, nothing can be returned from a runnable
	}

	if task.result != nil {
		return task.result // result has been cached to task.result
	}

	if len(task.resultChan) == 0 {
		return empty
	}

	result, ok := <-task.resultChan
	if !ok {
		return empty
	}
	task.result = result

	close(task.resultChan) // close the channels immediately after reading and caching the result

	return task.result
}

// Err returns an error from Task, it will block until the task is done.
func (task *Task) Err() error {
	return task.err
}

func (task *Task) execute(wg *sync.WaitGroup) {
	defer func() {
		if err := recover(); err != nil {
			task.err = errors.New(fmt.Sprint(err))
		}
		wg.Done()
	}()

	s := task.s
	r := task.r

	if s != nil {
		// supplier
		result := task.resultChan
		res, err := s.get()
		result <- res
		if err != nil {
			task.err = err
			return
		}
	}

	if r != nil {
		// runnable
		if err := r.run(); err != nil {
			task.err = err
			return
		}
	}
}

type supplier struct {
	get func() (any, error)
}

type runnable struct {
	run func() error
}
