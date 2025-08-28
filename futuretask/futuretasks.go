package futuretask

import "sync"

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
func Execute(futures ...*Task) error {
	if len(futures) == 0 {
		return nil
	}

	var (
		wg sync.WaitGroup
		mu sync.RWMutex

		doneChan = make(chan bool)
		errChan  = make(chan error)
	)

	for _, future := range futures {
		wg.Add(1)

		go func(future *Task, wg *sync.WaitGroup, errChan chan error, mu *sync.RWMutex) {
			future.execute(wg)
			if future.err != nil {
				mu.Lock()
				errChan <- future.err
				mu.Unlock()
			}
		}(future, &wg, errChan, &mu)
	}

	go func() {
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()

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
