package concurrent_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/AyakuraYuki/go-concurrent/concurrent"
)

func TestTask(t *testing.T) {
	task := concurrent.Run(func() error {
		time.Sleep(2 * time.Second)
		return nil
	})

	ticker := time.NewTicker(500 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			t.Logf("work... (ut: %s)", task.SpendTime())
			if task.IsDone() {
				ticker.Stop()
				t.Fatal("unexpected task state")
			}
		default:
			if task.IsDone() {
				t.Logf("task is done (ut: %s)", task.SpendTime())
				ticker.Stop()
				return
			}
		}
	}
}

func TestTask_State(t *testing.T) {
	task := concurrent.Run(func() error {
		time.Sleep(1 * time.Second)
		return nil
	})

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, concurrent.StateRunning, task.State(), "expected running state but was not")

	task.Wait()
	assert.True(t, task.IsDone(), "expected task to be done but was not")
}

func TestTask_Result(t *testing.T) {
	taskA := concurrent.Get(func() (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "A", nil
	})

	taskB := concurrent.Get(func() (int64, error) {
		time.Sleep(500 * time.Millisecond)
		return 2233, nil
	})

	type foo struct {
		Name string `json:"name"`
	}
	taskC := concurrent.Get(func() (*foo, error) {
		time.Sleep(1 * time.Second)
		return &foo{Name: "bilibili"}, nil
	})

	st := time.Now()

	a, err := taskA.Result()
	assert.NoError(t, err)
	assert.EqualValues(t, "A", a)

	b, err := taskB.Result()
	assert.NoError(t, err)
	assert.EqualValues(t, 2233, b)

	c, err := taskC.Result()
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.EqualValues(t, "bilibili", c.Name)

	assert.EqualValues(t, 1, int(time.Since(st).Seconds()), "should not take over 3 seconds")
}

func TestTask_Get(t *testing.T) {
	task := concurrent.Get(func() (string, error) {
		time.Sleep(1 * time.Second)
		return "A", nil
	})

	st := time.Now()
	res1 := task.Get()
	assert.EqualValues(t, "A", res1)
	assert.EqualValues(t, 1, int(time.Since(st).Seconds()))

	st = time.Now()
	res2 := task.Get()
	assert.EqualValues(t, "A", res2)
	assert.True(t, time.Since(st).Seconds() < 1.0)

	st = time.Now()
	res3 := task.Get()
	assert.EqualValues(t, "A", res3)
	assert.True(t, time.Since(st).Seconds() < 1.0)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := task.Get()
			assert.EqualValues(t, "A", res)
			t.Log(res, time.Now().UnixMilli())
		}()
	}
	wg.Wait()
}

func TestTask_Err(t *testing.T) {
	taskA := concurrent.Run(func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	taskB := concurrent.Run(func() error {
		time.Sleep(200 * time.Millisecond)
		return errors.New("error")
	})

	taskC := concurrent.Get(func() (string, error) {
		time.Sleep(300 * time.Millisecond)
		return "", errors.New("error")
	})

	taskD := concurrent.Run(func() error {
		panic("PANIC")
	})

	if err := taskA.Err(); err != nil {
		t.Error("unexpected error raised from task A")
	}

	if err := taskB.Err(); err == nil {
		t.Error("want an error raised from task B but not")
	}

	if err := taskC.Err(); err == nil {
		t.Error("want an error raised from task C but not")
	}

	if err := taskD.Err(); err == nil {
		t.Error("want a panic raised from task D but not")
	}
}

func TestTask_panicRecover(t *testing.T) {
	task := concurrent.Run(func() error {
		time.Sleep(1 * time.Second)
		panic("panic")
	})

	_, err := task.Result()
	assert.Error(t, err)
	assert.EqualValues(t, "panic", err.Error())
}

func TestTask_write(t *testing.T) {
	var (
		times   = 1000000
		tasks   = make([]*concurrent.Task[any], 0)
		counter atomic.Int64
	)
	for i := 0; i < times; i++ {
		tasks = append(tasks, concurrent.Run(func() error {
			counter.Add(int64(1))
			return nil
		}))
	}
	concurrent.WaitAll(tasks...)
	if c := counter.Load(); c != int64(times) {
		t.Fatalf("counter does not match: %d", c)
	}
}

func TestTask_stabilize_inWaitGroup(t *testing.T) {
	var (
		times         = 10000
		tasksPreBatch = 500
		wg            sync.WaitGroup
	)
	for i := 0; i < times; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tasks := make([]*concurrent.Task[int], 0)
			for k := 0; k < tasksPreBatch; k++ {
				tasks = append(tasks, concurrent.Get(func() (int, error) {
					return i + k, nil
				}))
			}
			concurrent.WaitAll(tasks...)
		}()
	}
	wg.Wait()
	t.Log("no panic, no deadlock, passed")
}

func TestTask_Metrics(t *testing.T) {
	concurrent.ResetMetricsStatus()

	taskA := concurrent.Run(func() error {
		time.Sleep(300 * time.Millisecond)
		return nil
	})

	taskB := concurrent.Run(func() error {
		time.Sleep(100 * time.Millisecond)
		return errors.New("error")
	})

	taskC := concurrent.Get(func() (int, error) {
		time.Sleep(200 * time.Millisecond)
		return 2233, nil
	})

	taskA.Wait()
	taskB.Wait()
	taskC.Wait()

	metrics := concurrent.MetricsStatus()
	assert.EqualValues(t, 3, metrics.Created)
	assert.EqualValues(t, 2, metrics.Done)
	assert.EqualValues(t, 1, metrics.Failed)
}
