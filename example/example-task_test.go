package example

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/AyakuraYuki/go-concurrent/concurrent"
)

// case 1: run async
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

// case 2: supply async
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
