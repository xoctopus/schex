package schex_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xoctopus/x/misc/must"

	schex2 "github.com/xoctopus/schex"
)

var count atomic.Int64

type MockHandler[T any] struct{}

func (MockHandler[T]) Do(ctx context.Context, v T) error {
	count.Add(-1)
	return nil
}

func BenchmarkPushConcurrent(b *testing.B) {
	ctx := context.Background()

	s := schex2.NewScheduler[int](
		schex2.JobFunc[int](func(context.Context, int) error { return nil }),
		schex2.WithoutPendingLimitation[int](),
		schex2.WithParallel[int](8),
	)

	must.NoError(s.Run(ctx))
	defer s.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// test push benchmark
		for pb.Next() {
			_ = s.Push(ctx, 1)
		}
	})
}

func bench[T any](b *testing.B, options ...schex2.SchedulerOptionApplier[T]) {
	ctx := context.Background()

	s := schex2.NewScheduler[T](&MockHandler[T]{}, options...)

	_ = s.Run(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count.Add(1)
		_ = s.Push(ctx, *new(T))
	}

	for count.Load() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	b.StopTimer()
	s.Close()
}

func Benchmark(b *testing.B) {
	b.Run("LIFO", func(b *testing.B) {
		bench(
			b,
			schex2.WithoutPendingLimitation[int](),
			schex2.WithParallel[int](16),
			schex2.WithLifoScheduleMode[int](),
		)
	})

	b.Run("FIFO", func(b *testing.B) {
		bench(
			b,
			schex2.WithoutPendingLimitation[int](),
			schex2.WithParallel[int](16),
			schex2.WithFifoScheduleMode[int](),
		)
	})

	b.Run("Concurrency100", func(b *testing.B) {
		bench(
			b,
			schex2.WithoutPendingLimitation[int](),
			schex2.WithParallel[int](100),
			schex2.WithFifoScheduleMode[int](),
		)
	})

	b.Run("Concurrency1000", func(b *testing.B) {
		bench(
			b,
			schex2.WithoutPendingLimitation[int](),
			schex2.WithParallel[int](1000),
			schex2.WithFifoScheduleMode[int](),
		)
	})

	b.Run("Concurrency10000", func(b *testing.B) {
		bench(
			b,
			schex2.WithoutPendingLimitation[int](),
			schex2.WithParallel[int](10000),
			schex2.WithFifoScheduleMode[int](),
		)
	})
}
