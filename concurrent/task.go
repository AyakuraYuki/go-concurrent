package concurrent

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// State is the current Task state.
type State int32

const (
	StatePending   State = iota // task is pending
	StateRunning                // task is running
	StateDone                   // task is completed without error
	StateFailed                 // task is failed
	StateCancelled              // task is cancelled
)

// Task defines a unit of future task.
type Task[T any] struct {
	get func() (T, error) // get is the supplier function
	run func() error      // run is the runnable function

	result T     // result is the returned object from get
	err    error // err is the error raised from get or run

	state atomic.Int32 // state stores the task State

	mu   sync.RWMutex
	cond *sync.Cond
	once sync.Once

	startTime time.Time
	endTime   time.Time
}

// State resulting the current task state.
func (t *Task[T]) State() State {
	return State(t.state.Load())
}

// IsRunning reports the task is now running or not.
func (t *Task[T]) IsRunning() bool {
	return t.State() == StateRunning
}

var doneStates = []State{
	StateDone,
	StateFailed,
	StateCancelled,
}

// IsDone reports the task is now completed or still pending or running.
func (t *Task[T]) IsDone() bool {
	return slices.Contains(doneStates, t.State())
}

// SpendTime returns the time taken by the task from the start to the end of
// execution.
func (t *Task[T]) SpendTime() time.Duration {
	if t.startTime.IsZero() {
		return 0
	}

	endTime := t.endTime
	if endTime.IsZero() {
		endTime = time.Now()
	}

	return endTime.Sub(t.startTime)
}

// Result returns the result by waiting for the task to be done. If the task
// has been completed, returns the result directly.
func (t *Task[T]) Result() (result T, err error) {
	t.mu.RLock()

	if t.IsDone() {
		result, err = t.result, t.err
		t.mu.RUnlock()
		return result, err
	}

	t.mu.RUnlock()
	t.cond.L.Lock()
	defer t.cond.L.Unlock()

	done := make(chan bool, 1)
	defer close(done)
	go func() {
		for !t.IsDone() {
			t.cond.Wait()
		}
		done <- true
	}()

	<-done
	return t.result, t.err
}

// Get the result from Task and ignore the error.
// Note that a runnable Task has no result.
func (t *Task[T]) Get() (result T) {
	result, _ = t.Result()
	return result
}

// Err returns the error raised from Task.
func (t *Task[T]) Err() (err error) {
	_, err = t.Result()
	return err
}

// Wait for the task to be done, just wait...
func (t *Task[T]) Wait() {
	_, _ = t.Result()
}

func (t *Task[T]) execute() {
	t.once.Do(func() {

		defer func() {
			if err := recover(); err != nil {
				t.err = errors.Join(t.err, errors.New(fmt.Sprint(err)))
				t.state.Store(int32(StateFailed))
				atomic.AddInt64(&globalMatrics.Failed, 1) // by panic
				t.cond.L.Unlock()
			}
			t.endTime = time.Now()
			t.cond.Broadcast()
		}()

		t.cond.L.Lock()
		t.startTime = time.Now()
		t.state.Store(int32(StateRunning))
		t.cond.L.Unlock()

		t.cond.L.Lock()
		if t.get != nil {
			t.result, t.err = t.get()
		} else if t.run != nil {
			t.err = t.run()
		}
		if t.err != nil {
			t.state.Store(int32(StateFailed))
			atomic.AddInt64(&globalMatrics.Failed, 1)
		} else {
			t.state.Store(int32(StateDone))
			atomic.AddInt64(&globalMatrics.Done, 1)
		}
		t.cond.L.Unlock()

	})
}

type TaskMatrics struct {
	Created   int64
	Done      int64
	Failed    int64
	Cancelled int64
}

var globalMatrics TaskMatrics

// MetricsStatus returns the metrics status
func MetricsStatus() TaskMatrics {
	return globalMatrics
}

// ResetMetricsStatus can reset the metrics data
func ResetMetricsStatus() {
	globalMatrics.Created = 0
	globalMatrics.Done = 0
	globalMatrics.Failed = 0
	globalMatrics.Cancelled = 0
}
