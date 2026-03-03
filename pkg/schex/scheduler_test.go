package schex_test

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/xoctopus/x/misc/must"
	. "github.com/xoctopus/x/testx"

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

	for parallel := 100; parallel <= schex.MaxParallel; parallel *= 10 {
		b.Run("FIFO_x"+strconv.Itoa(parallel), func(b *testing.B) {
			bench(
				b,
				schex.WithoutPendingLimitation[int](),
				schex.WithParallel[int](parallel+1),
				schex.WithFifoScheduleMode[int](),
				schex.WithCloseTimeout[int](timeout),
			)
		})
		runtime.GC()
		b.Run("LIFO_x"+strconv.Itoa(parallel), func(b *testing.B) {
			bench(
				b,
				schex.WithoutPendingLimitation[int](),
				schex.WithParallel[int](parallel+1),
				schex.WithLifoScheduleMode[int](),
				schex.WithCloseTimeout[int](timeout),
			)
		})
		runtime.GC()
	}
}

func TestScheduler(t *testing.T) {
	expect := errors.New("callback")
	s := schex.NewScheduler(
		schex.JobFunc[int](func(ctx context.Context, v int) error {
			if v == 2 {
				return expect
			}
			return nil
		}),
		schex.WithCallback(func(v int, err error) {
			if errors.Is(err, expect) {
				Expect(t, v, Equal(2))
			}
		}),
		schex.WithExitCallback(func(pending []int, err error) {
			Expect(t, err, IsCodeError(schex.ERROR__SCHEDULER_CANCELED))
		}),
		schex.WithMaxPending[int](2),
		schex.WithoutDetached[int](),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	Expect(t, s.Run(ctx), Succeed())
	Expect(t, s.Push(ctx, 2), Succeed())
	Expect(t, s.Push(ctx, 1), Succeed())
	Expect(t, s.Close(), Succeed())

	s = schex.NewScheduler(
		schex.JobFunc[int](func(ctx context.Context, v int) error {
			switch v {
			case 1:
				panic(expect)
			default:
				panic("string")
			}
		}),
		schex.WithCallback(func(v int, err error) {
			Expect(t, err, IsCodeError(schex.ERROR__SCHEDULER_JOB_PANICKED))
			switch v {
			case 1:
				Expect(t, err, IsError(expect))
			default:
				Expect(t, err, ErrorContains("string"))
			}
		}),
		schex.WithMaxPending[int](2),
	)
	ctx = context.Background()
	Expect(t, s.Push(ctx, 1), Succeed())
	Expect(t, s.Push(ctx, 2), Succeed())
	Expect(t, s.Push(ctx, 3), IsCodeError(schex.ERROR__REACH_MAX_PENDING))

	Expect(t, s.Run(ctx), Succeed())
	Expect(t, s.Run(ctx), IsCodeError(schex.ERROR__SCHEDULER_RERUN))
	time.Sleep(2 * time.Second)

	Expect(t, s.Close(), Succeed())
	Expect(t, s.Push(ctx, 1), IsCodeError(schex.ERROR__SCHEDULER_CANCELED))
}
