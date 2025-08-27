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
func ExampleRunAsync() {
	futureA := concurrent.RunAsync(func() error {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		fmt.Printf("in future a, generated a random number %d\n", rand.Intn(100))
		return nil
	})

	futureB := concurrent.RunAsync(func() error {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		fmt.Println("in future b, greeting developer")
		return nil
	})

	futureC := concurrent.RunAsync(func() error {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		fmt.Println("in future c, we raised an error")
		return errors.New("hi guys")
	})

	// Feel free to wait anywhere until the CompletableFuture is completed.
	fmt.Println("do something...")
	concurrent.Wait(futureA, futureC)
	fmt.Println("after future A and C done, do something...")
	futureB.Wait() // waiting for future B
	fmt.Println("all futures done")

	// Or wait for all CompletableFutures done
	concurrent.Wait(futureA, futureB, futureC)

	// Handle errors from CompletableFuture
	if err := futureC.Err(); err != nil {
		log.Fatalf("error from future C: %v\n", err)
	}
}

// case 2: supply async
func ExampleSupplyAsync() {
	futureA := concurrent.SupplyAsync(func() (string, error) {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		return "bilibili", nil
	})

	futureB := concurrent.SupplyAsync(func() (int64, error) {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		return 2233, nil
	})

	type foo struct {
		Val string `json:"val"`
	}

	futureC := concurrent.SupplyAsync(func() (foo, error) {
		return foo{Val: "66"}, errors.New("this is an error")
	})

	futureD := concurrent.SupplyAsync(func() (*foo, error) {
		return &foo{Val: "2233"}, nil
	})

	// // Be warned, CompletableFutures with different generic types are not allowed to wait together use [concurrent.Wait].
	// //
	// //	Compile failed: Cannot use 'futureB' (type *CompletableFuture[int64]) as the type *CompletableFuture[string]
	// //	Compile failed: Cannot use 'futureC' (type *CompletableFuture[foo]) as the type *CompletableFuture[string]
	// //	Compile failed: Cannot use 'futureD' (type *CompletableFuture[*foo]) as the type *CompletableFuture[string]
	// //
	// concurrent.Wait(futureA, futureB, futureC, futureD)

	// Use [CompletableFuture.Get] to get result and ignore error
	title := futureA.Get()
	fmt.Printf("title: %s\n", title)

	// Use [CompletableFuture.Result] to get result and error
	number, err := futureB.Result()
	if err != nil {
		log.Fatalf("error from futureB: %v\n", err)
	}
	fmt.Printf("number: %d\n", number)

	// Use [CompletableFuture.Err] to handle error
	if err = futureC.Err(); err != nil {
		log.Fatalf("error from futureC: %v\n", err)
	}

	ptrFoo, err := futureD.Result()
	if err != nil {
		log.Fatalf("error from futureD: %v\n", err)
	}
	fmt.Printf("ptrFoo: %#v\n", ptrFoo)
}
