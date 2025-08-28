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

// Result returns both result and error from Task, it will block until the task is done.
func (task *Task) Result() (any, error) {
	result := task.Get()
	err := task.Err()
	return result, err
}

// Get the result from Task and ignore the error, it will block until the task is done.
// Note that a runnable Task has no result.
func (task *Task) Get() (empty any) {
	if task.get == nil {
		return empty // only supplier will return result, nothing can be returned from a runnable
	}

	task.mu.RLock()

	if task.result != nil {
		defer task.mu.RUnlock()
		return task.result // result has been cached to task.result
	}

	if len(task.resultChan) == 0 {
		defer task.mu.RUnlock()
		return empty
	}

	task.mu.RUnlock()
	task.mu.Lock()
	defer task.mu.Unlock()

	result, ok := <-task.resultChan
	if !ok {
		return empty
	}

	task.result = result
	return task.result
}

// Err returns an error from Task, it will block until the task is done.
func (task *Task) Err() error {
	return task.err
}

func (task *Task) execute(wg *sync.WaitGroup) {
	task.once.Do(func() {

		defer func() {
			if err := recover(); err != nil {
				task.err = errors.Join(task.err, errors.New(fmt.Sprint(err)))
			}
			if task.resultChan != nil {
				close(task.resultChan)
			}
			wg.Done()
		}()

		get := task.get
		run := task.run

		if get != nil {

			res, err := get()
			task.resultChan <- res

			if err != nil {
				task.err = err
			}

		} else if run != nil {

			err := run()
			if err != nil {
				task.err = err
			}

		}

	})
}
