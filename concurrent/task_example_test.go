package concurrent_test

import (
	"fmt"
	"time"

	"github.com/AyakuraYuki/go-concurrent/concurrent"
)

func ExampleTask() {
	taskA := concurrent.Run(func() error {
		time.Sleep(1 * time.Second)
		fmt.Println("hello, world")
		return nil
	})

	taskB := concurrent.Get(func() (string, error) {
		time.Sleep(500 * time.Millisecond)
		fmt.Println("bilibili")
		return "2233", nil
	})

	taskA.Wait()
	str, err := taskB.Result()
	if err != nil {
		panic(err)
	}

	fmt.Println(str)

	// Output:
	// bilibili
	// hello, world
	// 2233
}

func ExampleTask_runnable() {
	task := concurrent.Run(func() error {
		time.Sleep(3 * time.Second)
		return nil
	})

	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			fmt.Println("tick")
		default:
			if task.IsDone() {
				_, err := task.Result()
				fmt.Printf("%v", err)
				ticker.Stop()
				return
			}
		}
	}

	// Output:
	// tick
	// tick
	// tick
	// <nil>
}
