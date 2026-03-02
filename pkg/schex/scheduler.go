package schex

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xoctopus/x/codex"
	"github.com/xoctopus/x/container/queue"
	"github.com/xoctopus/x/container/stack"
	"github.com/xoctopus/x/misc/must"
)

func NewScheduler[T any](fn Job[T], appliers ...SchedulerOptionApplier[T]) Scheduler[T] {
	must.BeTrueF(fn != nil, "job handler is required")
	s := &scheduler[T]{
		option: option[T]{
			maxPending: 1,
			parallel:   1,
			mode:       FIFO,
		},
		fn:     fn,
		cond:   sync.NewCond(&sync.Mutex{}),
		exited: make(chan struct{}, 1),
	}
	for _, applier := range appliers {
		applier(&s.option)
	}
	switch s.mode {
	case FIFO:
		s.tasks = queue.NewSafeQueue[T]()
	case LIFO:
		s.tasks = stack.NewSafeStack[T]()
	}

	if cb := s.userExitCallback; cb != nil {
		s.scheExitCallback = func(reason error) {
			jobs := make([]T, 0, s.tasks.Len())
			s.tasks.Range(func(v T) bool {
				jobs = append(jobs, v)
				return true
			})
			cb(jobs, reason)
		}
	}

	return s
}

type scheduler[T any] struct {
	option[T]

	cond *sync.Cond
	// fn job handler
	fn Job[T]
	// tasks task list
	tasks Tasks[T]
	// pending atomic counter for pending tasks
	pending atomic.Int64
	// wg holding all parallel go routines
	wg sync.WaitGroup
	// running if scheduler is running
	running atomic.Bool
	// closing if scheduler is closing
	closing atomic.Bool
	// exited signal for scheduler finishing exiting and cleaning up when closing
	// gracefully
	exited chan struct{}
	// onceClose Close idempotency
	onceClose sync.Once
	// cancel cancel scheduler context callback
	cancel context.CancelCauseFunc
}

func (s *scheduler[T]) Push(_ context.Context, v T) error {
	if s.closing.Load() {
		return codex.New(ERROR__SCHEDULER_CANCELED)
	}

	if s.maxPending < 0 {
		s.pending.Add(1)
		goto Append
	}

	for {
		current := s.pending.Load()
		if current >= int64(s.maxPending) {
			return codex.New(ERROR__REACH_MAX_PENDING)
		}
		if s.pending.CompareAndSwap(current, current+1) {
			goto Append
		}
	}

Append:
	s.tasks.Push(v)
	s.cond.Broadcast()
	// s.cond.Signal()
	return nil
}

func (s *scheduler[T]) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return codex.New(ERROR__SCHEDULER_RERUN)
	}
	ctx, s.cancel = context.WithCancelCause(ctx)
	go s.shutdown(ctx)
	for range s.parallel {
		s.wg.Go(func() {
			s.run(ctx)
		})
	}
	return nil
}

func (s *scheduler[T]) shutdown(ctx context.Context) {
	<-ctx.Done()
	if s.closing.CompareAndSwap(false, true) {
		defer close(s.exited)

		waited := make(chan struct{}, 1)
		// wait all scheduling routines exiting
		go func() {
			s.wg.Wait()
			waited <- struct{}{}
		}()
		// wake up all hanging scheduling routines for exiting
	Loop:
		for {
			select {
			case <-waited:
				break Loop
			default:
				s.cond.Broadcast()
				time.Sleep(20 * time.Millisecond)
			}
		}
		// do clean up after exiting
		if s.exitCbCalled.CompareAndSwap(false, true) {
			if s.scheExitCallback != nil {
				s.scheExitCallback(context.Cause(ctx))
			}
		}
		s.exited <- struct{}{}
	}
}

func (s *scheduler[T]) run(ctx context.Context) {
	for {
		s.cond.L.Lock()
		// avoid spurious waking up
		for s.tasks.Len() == 0 && !s.closing.Load() {
			s.cond.Wait()
		}
		if s.closing.Load() {
			s.cond.L.Unlock()
			return
		}
		v, ok := s.tasks.Pop()
		s.cond.L.Unlock()

		if ok {
			// if job was popped and is handling. the context should not be affected
			// by this cancel. the context input just control lifetime of each runner
			s.pending.Add(-1)
			s.do(context.WithoutCancel(ctx), v)
		}
	}
}

func (s *scheduler[T]) do(ctx context.Context, v T) {
	var err error
	defer func() {
		if x := recover(); x != nil {
			err = codex.New(ERROR__SCHEDULER_JOB_PANICKED)
		}
		if s.callback != nil {
			s.callback(v, err)
		}
	}()
	err = s.fn.Do(ctx, v)
}

func (s *scheduler[T]) Close() {
	s.onceClose.Do(
		func() {
			s.cancel(codex.New(ERROR__SCHEDULER_CANCELED))
			<-s.exited
		},
	)
}
