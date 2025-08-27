package concurrent

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// CompletableFuture defines a unit of future tasks and allows the running of a supplier/runnable function.
type CompletableFuture[T any] struct {
	s          *supplier[T] // supplier future
	resultChan chan T       // result channel
	result     *T           // result holder

	r *runnable // runnable future

	done atomic.Bool
	err  error // error holder
}

// Result returns both result and error from CompletableFuture, it will block until the task is done.
func (future *CompletableFuture[T]) Result() (t T, err error) {
	t = future.Get()
	err = future.Err()
	return
}

// Get the result from CompletableFuture and ignore the error, it will block until the task is done.
// Note that a runnable CompletableFuture has no result.
func (future *CompletableFuture[T]) Get() (t T) {
	// var empty T

	if future.s == nil {
		return t // only supplier will return result, nothing can be returned from a runnable
	}

	if future.result != nil {
		return *future.result // result has been cached to future.result
	}

	future.Wait() // wait for the task done

	if len(future.resultChan) == 0 {
		return t
	}

	result, ok := <-future.resultChan
	if !ok {
		return t
	}
	future.result = &result

	future.close() // close the channels immediately after reading and caching the result

	return result
}

// Err returns an error from CompletableFuture, it will block until the task is done.
func (future *CompletableFuture[T]) Err() error {
	future.Wait()
	return future.err
}

// IsDone indicates whether the CompletableFuture is done or not.
func (future *CompletableFuture[T]) IsDone() bool {
	return future.done.Load()
}

// Wait blocks the invoker until this CompletableFuture is done.
func (future *CompletableFuture[T]) Wait() {
	for !future.done.Load() {
		break
	}
}

func (future *CompletableFuture[T]) execute() {
	defer func() {
		if err := recover(); err != nil {
			future.err = errors.New(fmt.Sprint(err))
		}
		future.done.Store(true)
	}()

	if future == nil || (future.r == nil && future.s == nil) {
		future.close()
		return
	}

	s := future.s
	r := future.r

	if s != nil {
		// supplier
		resultChan := future.resultChan
		res, err := s.get()
		resultChan <- res
		if err != nil {
			future.err = err
			return
		}
	}

	if r != nil {
		// runnable
		if err := r.run(); err != nil {
			future.err = err
			return
		}
	}
}

func (future *CompletableFuture[T]) close() {
	if future == nil {
		return
	}
	if future.resultChan != nil {
		close(future.resultChan)
	}
}

type supplier[T any] struct {
	get func() (T, error)
}

type runnable struct {
	run func() error
}
