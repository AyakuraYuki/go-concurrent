package futuretask_test

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/AyakuraYuki/go-concurrent/futuretask"
)

func TestExecute(t *testing.T) {
	var (
		holder  = ""
		resultA any
	)

	futureA := futuretask.PlanSupply(func() (any, error) {
		time.Sleep(600 * time.Millisecond)
		t.Log("future a: done")
		return int64(2233), nil
	})

	futureB := futuretask.PlanRun(func() error {
		time.Sleep(300 * time.Millisecond)
		t.Log("future b: run async")
		return nil
	})

	futureC := futuretask.PlanRun(func() error {
		time.Sleep(450 * time.Millisecond)
		holder = "bilibili"
		t.Log(`future c: assigned holder by "bilibili"`)
		return nil
	})

	// execute
	if err := futuretask.Execute(futureA, futureB, futureC); err != nil {
		t.Fatalf("unexpected error raised: %v", err)
	}

	if resultA = futureA.Get(); resultA == nil {
		t.Fatalf("future a: result is nil")
	}
	if val, ok := resultA.(int64); !ok || val != int64(2233) {
		t.Fatalf("future a: result is not int64 or is not 2233 in int64")
	}

	if holder != "bilibili" {
		t.Fatalf(`future c: holder is not "bilibili" in string`)
	}
}

func TestRun(t *testing.T) {
	futureA := futuretask.PlanRun(func() error {
		time.Sleep(600 * time.Millisecond)
		t.Log("future a: done")
		return nil
	})

	futureB := futuretask.PlanRun(func() error {
		time.Sleep(200 * time.Millisecond)
		t.Log("future b: raise error")
		return errors.New("raise error")
	})

	futureC := futuretask.PlanSupply(func() (any, error) {
		time.Sleep(700 * time.Millisecond)
		t.Log("future c: return result and raise error")
		return int64(2233), errors.New("bilibili")
	})

	futuretask.Run(futureA, futureB, futureC)

	if err := futureA.Err(); err != nil {
		t.Fatalf("unexpected error raised from future a: %v", err)
	}

	if err := futureB.Err(); err == nil {
		t.Fatal("expected an error raised from future b, but got nothing")
	}

	resultC, err := futureC.Result()
	if resultC == nil {
		t.Fatalf("expected a result from future c, but got nil")
	}
	if val, ok := resultC.(int64); !ok || val != int64(2233) {
		t.Fatalf("future c: result is not int64 or is not 2233 in int64")
	}
	if err == nil {
		t.Fatalf("expected an error raised from future c, but got nothing")
	}
}

func TestTask_Get(t *testing.T) {
	// call Get() multiple times
	future := futuretask.PlanSupply(func() (any, error) {
		return int32(2233), nil
	})

	futuretask.Run(future)

	numA := future.Get()
	numB, ok := future.Get().(int32)
	assert.True(t, ok)
	if numA != numB {
		t.Fatalf("future a: result is not the same as future b")
	}
}

func TestTask_GetInGoroutine(t *testing.T) {
	future := futuretask.PlanSupply(func() (any, error) {
		return int32(2233), nil
	})

	futuretask.Run(future)

	var (
		wg sync.WaitGroup

		consume = func(v any) {
			if n, ok := v.(int32); ok {
				_ = rand.Int31n(n)
			}
		}
	)

	for i := 0; i < 100000; i++ {
		wg.Add(1)

		go func(future *futuretask.Task) {
			defer wg.Done()

			consume(future.Get())
		}(future)
	}

	wg.Wait()

	t.Log("no panic, no deadlock, no race, passed")
}

func TestExecute_stabilize_futuresInGoroutine(t *testing.T) {
	var (
		wg    sync.WaitGroup
		times = 1000
	)

	for i := 0; i < times; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			futures := make([]*futuretask.Task, 0)
			for j := 0; j < times; j++ {
				futures = append(futures, futuretask.PlanSupply(func() (any, error) {
					return j + 1, nil
				}))
			}

			if err := futuretask.Execute(futures...); err != nil {
				fmt.Println(err)
			}
		}()
	}

	wg.Wait()

	t.Log("no panic, no deadlock, passed")
}

func TestExecute_stabilize_accuracy(t *testing.T) {
	numbers := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	futures := make([]*futuretask.Task, 0)
	for i := 0; i < 100000; i++ {
		futures = append(futures, futuretask.PlanSupply(func() (any, error) {
			rn := rand.Intn(len(numbers))
			return []int{rn, numbers[rn]}, nil
		}))
	}
	if err := futuretask.Execute(futures...); err != nil {
		t.Fatal(err)
	}

	for _, future := range futures {
		ret := future.Get().([]int)
		if ret[1] != numbers[ret[0]] {
			t.Fatalf("unexpected random number ret: %v", ret)
		}
	}
}

func TestExecute_stabilize_write(t *testing.T) {
	var (
		atomicCounter atomic.Int64
		futures       []*futuretask.Task
		target        = int64(100000)
	)

	for i := int64(0); i < target; i++ {
		futures = append(futures, futuretask.PlanRun(func() error {
			atomicCounter.Add(1) // thread-safe write
			return nil
		}))
	}

	if err := futuretask.Execute(futures...); err != nil {
		t.Fatal(err)
	}

	if atomicCounter.Load() != target { // atomic counter should equal to 100000
		t.Fatalf("unexpected times in atomic counter")
	}
}

func TestRaceCondition_ChannelAccess(t *testing.T) {
	const numReaders = 100
	const expectedValue = "test-value-12345"

	task := futuretask.PlanSupply(func() (any, error) {
		// ensure the wait time for this goroutine
		time.Sleep(50 * time.Millisecond)
		return expectedValue, nil
	})

	var wg sync.WaitGroup
	results := make([]any, numReaders)

	go func() {
		time.Sleep(10 * time.Millisecond)
		futuretask.Run(task)
	}()

	// try to get result in multiple goroutines
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index] = task.Get()
		}(i)
	}

	wg.Wait()

	// verify equality
	for i, result := range results {
		if result != nil {
			assert.Equal(t, expectedValue, result, "Result mismatch at index %d", i)
		}
	}
}

func TestRaceCondition_ExecuteVsRun(t *testing.T) {
	const numTasks = 50

	var (
		tasks1       []*futuretask.Task
		tasks2       []*futuretask.Task
		expectedSum1 int64
		expectedSum2 int64
	)

	for i := 0; i < numTasks; i++ {
		value1 := int64(i + 1)
		value2 := int64((i + 1) * 10)
		expectedSum1 += value1
		expectedSum2 += value2

		task1 := futuretask.PlanSupply(func() (any, error) {
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			return value1, nil
		})

		task2 := futuretask.PlanSupply(func() (any, error) {
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			return value2, nil
		})

		tasks1 = append(tasks1, task1)
		tasks2 = append(tasks2, task2)
	}

	var (
		wg  sync.WaitGroup
		err error
	)

	wg.Add(2)

	go func() {
		defer wg.Done()
		err = futuretask.Execute(tasks1...)
	}()

	go func() {
		defer wg.Done()
		futuretask.Run(tasks2...)
	}()

	wg.Wait()

	assert.NoError(t, err)

	var (
		actualSum1 int64
		actualSum2 int64
	)

	for _, task := range tasks1 {
		if result := task.Get(); result != nil {
			actualSum1 += result.(int64)
		}
	}

	for _, task := range tasks2 {
		if result := task.Get(); result != nil {
			actualSum2 += result.(int64)
		}
	}

	assert.Equal(t, expectedSum1, actualSum1)
	assert.Equal(t, expectedSum2, actualSum2)
}

func TestRaceCondition_MixedOperations(t *testing.T) {
	const numOperations = 200

	var (
		allTasks   []*futuretask.Task
		tasksMutex sync.RWMutex
		wg         sync.WaitGroup
	)

	for i := 0; i < numOperations; i++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			var task *futuretask.Task

			if index%2 == 0 {

				task = futuretask.PlanSupply(func() (any, error) {
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
					// 10% to fail
					if rand.Float32() < 0.1 {
						return nil, errors.New("random error")
					}
					return fmt.Sprintf("result-%d", index), nil
				})

			} else {

				task = futuretask.PlanRun(func() error {
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
					// 10% to fail
					if rand.Float32() < 0.1 {
						return errors.New("random error")
					}
					return nil
				})

			}

			// append to task slice
			tasksMutex.Lock()
			allTasks = append(allTasks, task)
			tasksMutex.Unlock()

			// choose the execution randomly
			if rand.Float32() < 0.5 {
				go func() {
					futuretask.Run(task)
				}()
			} else {
				go func() {
					_ = futuretask.Execute(task)
				}()
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(1 * time.Second) // just wait more time

	// verify all tasks
	tasksMutex.RLock()
	tasks := make([]*futuretask.Task, len(allTasks))
	copy(tasks, allTasks)
	tasksMutex.RUnlock()

	var resultWg sync.WaitGroup
	for i, task := range tasks {
		resultWg.Add(1)
		go func(index int, task *futuretask.Task) {
			defer resultWg.Done()

			result := task.Get()
			err := task.Err()

			if err != nil {
				assert.Nil(t, result, "Task %d should not have result when error occurred", index)
			}
		}(i, task)
	}

	resultWg.Wait()
}

func TestRaceCondition_ChannelClosing(t *testing.T) {
	const numTasks = 100

	var (
		tasks []*futuretask.Task
		wg    sync.WaitGroup
	)

	for i := 0; i < numTasks; i++ {
		tasks = append(tasks, futuretask.PlanSupply(func() (any, error) {
			return rand.Int(), nil
		}))
	}

	wg.Add(2)

	// execution
	go func() {
		defer wg.Done()
		futuretask.Run(tasks...)
	}()

	// read immediately
	go func() {
		defer wg.Done()

		var readWg sync.WaitGroup

		for i, task := range tasks {
			readWg.Add(1)
			go func(index int, task *futuretask.Task) {
				defer readWg.Done()

				// read multiple times to test the behavior after the channel is closed
				for j := 0; j < 5; j++ {
					result := task.Get()
					if result != nil {
						assert.IsType(t, int(0), result, "Task %d result type mismatch", index)
					}
					runtime.Gosched()
				}
			}(i, task)
		}

		readWg.Wait()
	}()

	wg.Wait()
}

func TestExecute_PanicRecovery(t *testing.T) {
	future := futuretask.PlanSupply(func() (any, error) {
		return int64(2233), nil
	})

	panicFuture := futuretask.PlanRun(func() error {
		panic("panic simulation")
	})

	err := futuretask.Execute(future, panicFuture)
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestRun_PanicRecovery(t *testing.T) {
	future := futuretask.PlanSupply(func() (any, error) {
		return int64(2233), nil
	})

	panicFuture := futuretask.PlanRun(func() error {
		panic("panic simulation")
	})

	futuretask.Run(future, panicFuture)

	err := panicFuture.Err()
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestRaceCondition_PanicRecovery(t *testing.T) {
	const numTasks = 50

	var tasks []*futuretask.Task
	var panicCount int64

	// make some task that can panic
	for i := 0; i < numTasks; i++ {
		task := futuretask.PlanSupply(func() (any, error) {
			// 30% to panic
			if rand.Float32() < 0.3 {
				atomic.AddInt64(&panicCount, 1)
				panic(fmt.Sprintf("intentional panic %d", rand.Int()))
			}
			return fmt.Sprintf("success %d", rand.Int()), nil
		})
		tasks = append(tasks, task)
	}

	// execution
	err := futuretask.Execute(tasks...)

	if err != nil {
		assert.Contains(t, err.Error(), "panic", "Error should contain 'panic': %v", err)
	}

	// verify that all tasks can safely obtain results or errors
	var wg sync.WaitGroup
	for i, task := range tasks {
		wg.Add(1)
		go func(index int, task *futuretask.Task) {
			defer wg.Done()

			result := task.Get()
			taskErr := task.Err()

			if taskErr != nil {
				assert.Nil(t, result, "Task %d should not have result when error occurred", index)
			}
		}(i, task)
	}

	wg.Wait()
}
