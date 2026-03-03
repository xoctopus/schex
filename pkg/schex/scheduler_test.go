package schex_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/xoctopus/x/misc/must"

	"github.com/xoctopus/schex/pkg/schex"
)

type MockHandler[T any] struct{}

func (MockHandler[T]) Do(ctx context.Context, v T) error {
	return nil
}

func BenchmarkPushConcurrent(b *testing.B) {
	ctx := context.Background()

	s := schex.NewScheduler[int](
		schex.JobFunc[int](func(context.Context, int) error { return nil }),
		schex.WithoutPendingLimitation[int](),
		schex.WithParallel[int](8),
		schex.WithCloseTimeout[int](0),
	)

	must.NoError(s.Run(ctx))
	defer func() { _ = s.Close() }()

	b.ResetTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		// test push benchmark
		for pb.Next() {
			_ = s.Push(ctx, 1)
		}
	})
}

func bench[T any](b *testing.B, options ...schex.SchedulerOptionApplier[T]) {
	ctx := context.Background()

	s := schex.NewScheduler[T](&MockHandler[T]{}, options...)
	defer func() { _ = s.Close() }()

	must.NoError(s.Run(ctx))

	b.ResetTimer()
	for b.Loop() {
		_ = s.Push(ctx, *new(T))
	}

	for s.Pending() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	b.StopTimer()
}

func Benchmark(b *testing.B) {
	timeout := time.Duration(0) // time.Millisecond waiting all goroutines exiting
	b.Run("LIFO_x16", func(b *testing.B) {
		bench(
			b,
			schex.WithoutPendingLimitation[int](),
			schex.WithParallel[int](16),
			schex.WithLifoScheduleMode[int](),
			schex.WithCloseTimeout[int](timeout),
		)
	})

	b.Run("FIFO_x16", func(b *testing.B) {
		bench(
			b,
			schex.WithoutPendingLimitation[int](),
			schex.WithParallel[int](16),
			schex.WithFifoScheduleMode[int](),
			schex.WithCloseTimeout[int](timeout),
		)
	})

	for parallel := 100; parallel <= schex.MaxParallel*10000; parallel *= 10 {
		b.Run("FIFO_x"+strconv.Itoa(parallel), func(b *testing.B) {
			bench(
				b,
				schex.WithoutPendingLimitation[int](),
				schex.WithParallel[int](parallel),
				schex.WithFifoScheduleMode[int](),
				schex.WithCloseTimeout[int](timeout),
			)
		})
		b.Run("LIFO_x"+strconv.Itoa(parallel), func(b *testing.B) {
			bench(
				b,
				schex.WithoutPendingLimitation[int](),
				schex.WithParallel[int](parallel),
				schex.WithLifoScheduleMode[int](),
				schex.WithCloseTimeout[int](timeout),
			)
		})
	}
}
