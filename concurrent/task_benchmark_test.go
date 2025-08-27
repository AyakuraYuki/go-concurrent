package concurrent_test

import (
	"errors"
	"math/rand"
	"sync"
	"testing"

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
