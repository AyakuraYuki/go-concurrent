package concurrent

// RunAsync creates a CompletableFuture with runnable function,
// then executes the runnable immediately in async.
func RunAsync(f func() error) *CompletableFuture[any] {
	future := &CompletableFuture[any]{
		r: &runnable{run: f},
	}
	go future.execute()
	return future
}

// SupplyAsync creates a CompletableFuture with supplier function,
// then executes the supplier immediately in async.
// After supplier done, the result will be cached into future.result.
func SupplyAsync[T any](f func() (T, error)) *CompletableFuture[T] {
	future := &CompletableFuture[T]{
		s:          &supplier[T]{get: f},
		resultChan: make(chan T, 1),
	}
	go future.execute()
	return future
}

// Wait for all the *CompletableFuture[T] to complete execution.
// Each element in the array must be declared with the same type.
func Wait[T any](futures ...*CompletableFuture[T]) {
	if len(futures) == 0 {
		return
	}
	for i := range futures {
		futures[i].Wait()
	}
}
