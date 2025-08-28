package example

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/AyakuraYuki/go-concurrent/concurrent"
)

func ExampleRun() {
	futureA := concurrent.Run(func() error {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		fmt.Printf("in future a, generated a random number %d\n", rand.Intn(100))
		return nil
	})

	futureB := concurrent.Run(func() error {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		fmt.Println("in future b, greeting developer")
		return nil
	})

	futureC := concurrent.Run(func() error {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		fmt.Println("in future c, we raised an error")
		return errors.New("hi guys")
	})

	// Feel free to wait anywhere until the Task is completed.
	fmt.Println("do something...")
	concurrent.WaitAll(futureA, futureC)
	fmt.Println("after future A and C done, do something...")
	futureB.Wait() // waiting for future B
	fmt.Println("all futures done")

	// Or wait for all Task done
	concurrent.WaitAll(futureA, futureB, futureC)

	// Handle errors from Task
	if err := futureC.Err(); err != nil {
		log.Fatalf("error from future C: %v\n", err)
	}
}

func ExampleGet() {
	futureA := concurrent.Get(func() (string, error) {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		return "bilibili", nil
	})

	futureB := concurrent.Get(func() (int64, error) {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		return 2233, nil
	})

	type foo struct {
		Val string `json:"val"`
	}

	futureC := concurrent.Get(func() (foo, error) {
		return foo{Val: "66"}, errors.New("this is an error")
	})

	futureD := concurrent.Get(func() (*foo, error) {
		return &foo{Val: "2233"}, nil
	})

	// Use [Task.Get] to get result and ignore error
	title := futureA.Get()
	fmt.Printf("title: %s\n", title)

	// Use [Task.Result] to get result and error
	number, err := futureB.Result()
	if err != nil {
		log.Fatalf("error from futureB: %v\n", err)
	}
	fmt.Printf("number: %d\n", number)

	// Use [Task.Err] to handle error
	if err = futureC.Err(); err != nil {
		log.Fatalf("error from futureC: %v\n", err)
	}

	ptrFoo, err := futureD.Result()
	if err != nil {
		log.Fatalf("error from futureD: %v\n", err)
	}
	fmt.Printf("ptrFoo: %#v\n", ptrFoo)
}

func RunMultipleTasks() {
	// Create multiple tasks
	tasks := make([]*concurrent.Task[int], 10)
	for i := 0; i < 10; i++ {
		index := i
		tasks[i] = concurrent.Get(func() (int, error) {
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			return index * 2, nil
		})
	}

	// Wait for all tasks
	concurrent.WaitAll(tasks...)

	// Collect results
	for i, task := range tasks {
		result := task.Get()
		fmt.Printf("Task %d result: %d\n", i, result)
	}
}

func HandleErrors() {
	task := concurrent.Get(func() (string, error) {
		return "", errors.New("something went wrong")
	})

	result, err := task.Result()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Result:", result)
}

func MonitorTask() {
	task := concurrent.Run(func() error {
		time.Sleep(2 * time.Second)
		return nil
	})

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for !task.IsDone() {
		<-ticker.C
		fmt.Printf("work (ut: %s)\n", task.SpendTime())
	}

	fmt.Printf("task state: %v (ut: %v)\n", task.State(), task.SpendTime())
}
