package concurrent_test

import (
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/AyakuraYuki/go-concurrent/concurrent"
)

const (
	BenchmarkRunTimesPreB = 100_000
)

func benchmarkRunTask() error {
	rn := rand.Intn(100)
	if rn < 50 {
		return errors.New("simulated error")
	}
	return nil
}

func benchmarkGetTask() (int, error) {
	rn := rand.Intn(100)
	if rn < 50 {
		return 0, errors.New("simulated error")
	}
	return rn, nil
}

func BenchmarkRun(b *testing.B) {
	var (
		wg    sync.WaitGroup
		tasks []*concurrent.Task[any]
		times = BenchmarkRunTimesPreB
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(times)
		tasks = make([]*concurrent.Task[any], times)
		for j := 0; j < times; j++ {
			tasks[j] = concurrent.Run(func() error {
				defer wg.Done()
				return benchmarkRunTask()
			})
		}
		wg.Wait()
	}
}

func BenchmarkGet(b *testing.B) {
	var (
		wg    sync.WaitGroup
		tasks []*concurrent.Task[int]
		times = BenchmarkRunTimesPreB
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(times)
		tasks = make([]*concurrent.Task[int], times)
		for j := 0; j < times; j++ {
			tasks[j] = concurrent.Get(func() (int, error) {
				defer wg.Done()
				return benchmarkGetTask()
			})
		}
		wg.Wait()
	}
}

func BenchmarkRaceCondition_ConcurrentTaskCreation(b *testing.B) {
	concurrent.ResetMetricsStatus()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			task := concurrent.Get(func() (int, error) {
				return rand.Int(), nil
			})
			task.Get()
		}
	})

	b.StopTimer()
	metrics := concurrent.MetricsStatus()
	b.Logf("created: %d, done: %d, failed: %d", metrics.Created(), metrics.Done(), metrics.Failed())
}

func BenchmarkRaceCondition_ConcurrentResultAccess(b *testing.B) {
	task := concurrent.Get(func() (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "benchmark-result", nil
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result := task.Get()
			if result != "benchmark-result" {
				b.Errorf("Unexpected result: %v", result)
			}
		}
	})
}

func BenchmarkRaceCondition_MixedOperations(b *testing.B) {
	concurrent.ResetMetricsStatus()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			switch rand.Intn(4) {
			case 0:
				// fast task
				task := concurrent.Get(func() (int, error) {
					return rand.Int(), nil
				})
				task.Get()

			case 1:
				// slow task
				task := concurrent.Run(func() error {
					time.Sleep(time.Microsecond)
					return nil
				})
				task.Wait()

			case 2:
				// a task probably to fail
				task := concurrent.Get(func() (string, error) {
					if rand.Float32() < 0.1 {
						return "", errors.New("random error")
					}
					return "success", nil
				})
				_, _ = task.Result()

			case 3:
				// verify metrics
				metrics := concurrent.MetricsStatus()
				_ = metrics.Created() + metrics.Done() + metrics.Failed()
			}
		}
	})
}
