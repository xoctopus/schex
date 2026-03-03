package schex

import "sync/atomic"

type (
	HandlerCallback[T any] func(T, error)
	ExitingCallback[T any] func([]T, error)
)

// option defines scheduler option
type option[T any] struct {
	maxPending       int
	parallel         int
	callback         HandlerCallback[T]
	exitCbCalled     atomic.Bool
	userExitCallback ExitingCallback[T]
	scheExitCallback func(reason error)
	mode             ScheduleMode
}

type SchedulerOptionApplier[T any] func(*option[T])

// WithMaxPending sets the maximum number of pending tasks allowed.
// the default value is 1.
func WithMaxPending[T any](maxPending int) SchedulerOptionApplier[T] {
	return func(o *option[T]) {
		o.maxPending = maxPending
	}
}

// WithParallel sets the number of tasks processed in parallel.
// the default value is 1.
func WithParallel[T any](parallel int) SchedulerOptionApplier[T] {
	return func(o *option[T]) {
		o.parallel = parallel
	}
}

// WithoutPendingLimitation disables the pending-task limit by setting it to -1.
func WithoutPendingLimitation[T any]() SchedulerOptionApplier[T] {
	return func(o *option[T]) {
		o.maxPending = -1
	}
}

// WithCallback sets a callback that is invoked after task execution.
func WithCallback[T any](cb HandlerCallback[T]) SchedulerOptionApplier[T] {
	return func(o *option[T]) {
		o.callback = cb
	}
}

// WithExitCallback sets a callback invoked when the scheduler exits.
// It receives remaining pending jobs and the exit reason.
// The callback is guaranteed to be called at most once.
func WithExitCallback[T any](cb ExitingCallback[T]) SchedulerOptionApplier[T] {
	return func(o *option[T]) {
		o.userExitCallback = cb
	}
}

// WithFifoScheduleMode sets the scheduler mode to FIFO.
func WithFifoScheduleMode[T any]() SchedulerOptionApplier[T] {
	return func(o *option[T]) {
		o.mode = FIFO
	}
}

// WithLifoScheduleMode sets the scheduler mode to LIFO.
func WithLifoScheduleMode[T any]() SchedulerOptionApplier[T] {
	return func(o *option[T]) {
		o.mode = LIFO
	}
}
