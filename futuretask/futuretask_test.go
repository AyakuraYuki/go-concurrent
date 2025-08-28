package futuretask_test

import (
	"errors"
	"fmt"
	"math/rand"
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

func TestExecute_panicRecover(t *testing.T) {
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

func TestRun_panicRecover(t *testing.T) {
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
