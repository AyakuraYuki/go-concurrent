package concurrent

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunAsync_Wait(t *testing.T) {
	futureA := RunAsync(func() error {
		time.Sleep(500 * time.Millisecond)
		t.Log("future a: after 500ms")
		return nil
	})

	futureB := RunAsync(func() error {
		time.Sleep(200 * time.Millisecond)
		t.Log("future b: after 200ms")
		return nil
	})

	futureC := RunAsync(func() error {
		time.Sleep(100 * time.Millisecond)
		t.Log("future c: after 100ms, raise error")
		return errors.New("raise error")
	})

	// wait for all future done
	st := time.Now().UnixMilli()
	futureA.Wait()
	futureB.Wait()
	futureC.Wait()
	t.Logf("done (%dms)", time.Now().UnixMilli()-st)
}

func TestWait(t *testing.T) {
	futureA := RunAsync(func() error {
		time.Sleep(2 * time.Second)
		t.Log("future a: after 2s")
		return nil
	})

	futureB := RunAsync(func() error {
		time.Sleep(500 * time.Millisecond)
		t.Log("future b: after 500ms")
		return nil
	})

	futureC := RunAsync(func() error {
		time.Sleep(1 * time.Second)
		t.Log("future c: after 1s, raise error")
		return errors.New("raise error")
	})

	st := time.Now().UnixMilli()
	Wait(futureA, futureB, futureC)
	t.Logf("done (%dms)", time.Now().UnixMilli()-st)
}

func TestRunAsync_Get(t *testing.T) {
	future := RunAsync(func() error {
		return nil
	})
	Wait(future)
	if got := future.Get(); got != nil {
		t.Fatalf("future.Get() = %#v, want nil", got)
	}
}

func TestRunAsync_Err(t *testing.T) {
	futureA := RunAsync(func() error {
		time.Sleep(500 * time.Millisecond)
		t.Log("future a: after 500ms")
		return nil
	})

	futureB := RunAsync(func() error {
		time.Sleep(200 * time.Millisecond)
		t.Log("future b: after 200ms")
		return nil
	})

	futureC := RunAsync(func() error {
		time.Sleep(100 * time.Millisecond)
		t.Log("future c: after 100ms, raise error")
		return errors.New("raise error")
	})

	// wait and handle error from completed future
	st := time.Now().UnixMilli()
	if err := futureA.Err(); err != nil {
		t.Fatalf("unexpected error raised from future a: %v", err)
	}
	if err := futureB.Err(); err != nil {
		t.Fatalf("unexpected error raised from future b: %v", err)
	}
	if err := futureC.Err(); err == nil {
		t.Fatal("expect an error from future c, but got nil")
	}
	t.Logf("done (%dms)", time.Now().UnixMilli()-st)
}

func TestSupplyAsync_Get(t *testing.T) {
	futureA := SupplyAsync(func() (int64, error) {
		time.Sleep(500 * time.Millisecond)
		return 2233, nil
	})

	futureB := SupplyAsync(func() (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "bilibili", nil
	})

	type foo struct {
		Val string `json:"val"`
	}
	futureC := SupplyAsync(func() (*foo, error) {
		time.Sleep(1 * time.Second)
		return &foo{Val: "bilibili"}, nil
	})

	st := time.Now().UnixMilli()

	if res, err := futureA.Result(); err != nil {
		t.Fatalf("unexpected error raised from future a: %v", err)
	} else {
		t.Log("future a:", res)
	}

	if res, err := futureB.Result(); err != nil {
		t.Fatalf("unexpected error raised from future b: %v", err)
	} else {
		t.Log("future b:", res)
	}

	if res, err := futureC.Result(); err != nil {
		t.Fatalf("unexpected error raised from future c: %v", err)
	} else {
		t.Logf("future c: %#v", res)
	}

	t.Logf("done (%dms)", time.Now().UnixMilli()-st)
}

func TestSupplyAsync_GetMultipleTimes(t *testing.T) {
	type foo struct {
		Val string `json:"val"`
	}
	future := SupplyAsync(func() (*foo, error) {
		time.Sleep(1 * time.Second)
		return &foo{Val: "bilibili"}, nil
	})

	f1 := future.Get()
	t.Logf("f1: %#v", f1)
	f2 := future.Get()
	t.Logf("f2: %#v, equals to f1: %t", f2, f2 == f1)
	f3 := future.Get()
	t.Logf("f3: %#v, equals to f1: %t, equals to f2: %t", f3, f3 == f1, f3 == f2)
}

func TestRunAsync_stabilize_futuresInGoroutine(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(100)
	st := time.Now().UnixMilli()
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			futures := make([]*CompletableFuture[any], 0)
			for j := 0; j < 100; j++ {
				futures = append(futures, RunAsync(func() error {
					fmt.Printf("i: %d, j: %d\n", i, j)
					return nil
				}))
			}
			Wait(futures...)
		}()
	}
	wg.Wait()
	t.Logf("done (%dms)", time.Now().UnixMilli()-st)
	// success if there's no panic
}

func TestSupplyAsync_stabilize_futuresInGoroutine(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1000)
	st := time.Now().UnixMilli()
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			futures := make([]*CompletableFuture[int], 0)
			for j := 0; j < 1000; j++ {
				futures = append(futures, SupplyAsync(func() (int, error) {
					return i + j, nil
				}))
			}
			Wait(futures...)
		}()
	}
	wg.Wait()
	t.Logf("done (%dms)", time.Now().UnixMilli()-st)
	// success if there's no panic
}

func TestSupplyAsync_accuracy(t *testing.T) {
	numbers := []int{5, 4, 6, 1, 2, 8, 7, 9, 3, 0}

	futures := make([]*CompletableFuture[[]int], 0)
	st := time.Now().UnixMilli()
	for i := 0; i < 1000000; i++ {
		futures = append(futures, SupplyAsync(func() ([]int, error) {
			rn := rand.Intn(len(numbers))
			return []int{rn, numbers[rn]}, nil
		}))
	}
	Wait(futures...)
	t.Logf("spent (%dms)", time.Now().UnixMilli()-st)

	for i, future := range futures {
		ret := future.Get()
		if ret[1] != numbers[ret[0]] {
			t.Fatalf("unexpected random number ret: %v", ret)
		}
		if i < 10 {
			t.Log(ret, numbers[ret[0]]) // show top 10
		}
	}
}

func TestRunAsync_write(t *testing.T) {
	times := 1000000

	futures := make([]*CompletableFuture[any], 0)
	counter := 0
	for i := 0; i < times; i++ {
		futures = append(futures, RunAsync(func() error {
			counter += 1 // non-thread-safe write
			return nil
		}))
	}
	Wait(futures...)
	if counter > times { // counter should less or equals to times
		t.Fatalf("unexpected counter: %d", counter)
	}

	futures = make([]*CompletableFuture[any], 0)
	var atomicCounter atomic.Int64
	for i := 0; i < times; i++ {
		futures = append(futures, RunAsync(func() error {
			atomicCounter.Add(1)
			return nil
		}))
	}
	Wait(futures...)
	if atomicCounter.Load() != int64(times) { // atomic counter should equal to times
		t.Fatalf("unexpected counter: %d", atomicCounter.Load())
	}
}

func TestExecute_panicRecover(t *testing.T) {
	future := SupplyAsync(func() (int64, error) { return 2233, nil })
	panicFuture := RunAsync(func() error { panic("panic simulation") })
	time.Sleep(1 * time.Second)

	t.Log(future.Get())

	err := panicFuture.Err()
	if err == nil {
		t.Fatal("expected an error")
	}

	if !future.IsDone() {
		t.Fatal("expected future to be done")
	}
	if !panicFuture.IsDone() {
		t.Fatal("expected panic future to be done")
	}
}

func BenchmarkRunAsync(b *testing.B) {
	futures := make([]*CompletableFuture[any], 0)
	for i := 0; i < b.N; i++ {
		futures = append(futures, RunAsync(func() error {
			rn := rand.Intn(100)
			if rn < 50 {
				return nil
			} else {
				return errors.New("case error")
			}
		}))
	}
	Wait(futures...)
}

func BenchmarkSupplyAsync(b *testing.B) {
	futures := make([]*CompletableFuture[int], 0)
	for i := 0; i < b.N; i++ {
		futures = append(futures, SupplyAsync(func() (int, error) {
			return i, nil
		}))
	}
	Wait(futures...)
}
