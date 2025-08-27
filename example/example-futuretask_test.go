package example

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/AyakuraYuki/go-concurrent/futuretask"
)

// case 1: execute multiple tasks
func ExecuteMultipleTasks() {
	futureA := futuretask.PlanSupply(func() (any, error) {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		return rand.Intn(100), nil
	})

	futureB := futuretask.PlanSupply(func() (any, error) {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		return "bilibili", nil
	})

	futureC := futuretask.PlanRun(func() error {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		return errors.New("raise error")
	})

	// You can't get result from any CompletableFuture if [concurrent.Execute] returns an error.
	if err := futuretask.Execute(futureA, futureB, futureC); err != nil {
		// panic(err) // Usually you should return the error, or panic it, or write the error message to logger.

		fmt.Println("caught error:", err) // For this demo, I will print the error message.
	}

	// DO NOT do the following operations, the program will panic!!!
	// resultA := futureA.Get().(int)
	// resultB := futureB.Get().(string)

	// But feel free to call [CompletableFuture.Err] to handle the error in CompletableFuture,
	// it is safe.
	if err := futureC.Err(); err != nil {
		fmt.Println("error from future c:", err)
	}
}

// case 2: run multiple tasks and handle errors manually
func Example_runMultipleTasks() {
	futureA := futuretask.PlanSupply(func() (any, error) {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		return rand.Intn(100), nil
	})

	futureB := futuretask.PlanSupply(func() (any, error) {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		return "bilibili", nil
	})

	futureC := futuretask.PlanRun(func() error {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		return errors.New("raise error")
	})

	// [concurrent.Run] executes futures without return error,
	// so you should handle error MANUALLY in each CompletableFuture.
	futuretask.Run(futureA, futureB, futureC)

	resultA, errA := futureA.Result()
	if errA != nil {
		fmt.Println("err a:", errA) // do something with error
	} else {
		fmt.Println("result a:", resultA)
	}

	resultB, errB := futureB.Result()
	if errB != nil {
		fmt.Println("err b:", errB) // do something with error
	} else {
		fmt.Println("result b:", resultB)
	}

	resultC, errC := futureC.Result()
	if errC != nil {
		fmt.Println("err c:", errC) // do something with error
	} else {
		fmt.Println("result c:", resultC)
	}
}
