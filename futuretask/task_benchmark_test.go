package futuretask_test

import (
	"math/rand"
	"testing"

	"github.com/AyakuraYuki/go-concurrent/futuretask"
)

func BenchmarkExecute(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	futures := make([]*futuretask.Task, 0)
	for i := 0; i < b.N; i++ {
		futures = append(futures, futuretask.PlanSupply(func() (any, error) {
			return rand.Intn(1000) + 1, nil
		}))
	}
	b.ResetTimer()
	_ = futuretask.Execute(futures...)
}
