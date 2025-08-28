package futuretask

import (
	"errors"
	"fmt"
	"sync"
)

// Task defines a unit of future tasks and allow the running of supplier/runnable function.
type Task struct {
	get func() (any, error) // supplier future
	run func() error        // runnable future

	resultChan chan any // result channel
	result     any      // result holder

	err error // error holder

	mu   sync.RWMutex
	once sync.Once
}

// Get the result from Task and ignore the error, it will block until the task is done.
// Note that a runnable Task has no result.
func (task *Task) Get() (empty any) {
	if task.get == nil {
		// only the [task.get] can get result, there is nothing from [task.run]
		return empty
	}

	task.mu.RLock()
	if task.result != nil {
		// result has been cached to task.result
		defer task.mu.RUnlock()
		return task.result
	}
	task.mu.RUnlock()

	task.mu.Lock()
	defer task.mu.Unlock()

	// double-check after acquiring write lock
	if task.result != nil {
		return task.result
	}

	select {
	case result, ok := <-task.resultChan:
		if !ok {
			return empty
		}
		task.result = result
		return task.result

	default:
		return empty
	}
}

// Err returns an error from Task, it will block until the task is done.
func (task *Task) Err() (err error) {
	task.mu.RLock()
	defer task.mu.RUnlock()

	return task.err
}

// Result returns both result and error from Task, it will block until the task is done.
func (task *Task) Result() (result any, err error) {
	result = task.Get()
	err = task.Err()
	return
}

func (task *Task) execute() error {
	task.once.Do(func() {

		defer func() {
			if err := recover(); err != nil {
				task.err = errors.Join(task.err, errors.New(fmt.Sprint(err)))
			}
			if task.resultChan != nil {
				close(task.resultChan)
			}
		}()

		task.mu.Lock()
		defer task.mu.Unlock()

		if task.get != nil {

			res, err := task.get()
			task.resultChan <- res

			if err != nil {
				task.err = err
			}

		} else if task.run != nil {

			err := task.run()
			if err != nil {
				task.err = err
			}

		}

	})

	return task.Err()
}
