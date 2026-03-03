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

	"github.com/xoctopus/schex/pkg/synapse"
)

func NewScheduler[T any](fn Job[T], appliers ...SchedulerOptionApplier[T]) Scheduler[T] {
	must.BeTrueF(fn != nil, "job handler is required")
	s := &scheduler[T]{
		option: option[T]{
			maxPending:   1,
			parallel:     1,
			mode:         FIFO,
			closeTimeout: 3 * time.Second,
		},
		fn:   fn,
		cond: sync.NewCond(&sync.Mutex{}),
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
	syn  synapse.Synapse

	// fn job handler
	fn Job[T]
	// tasks task list
	tasks Tasks[T]
	// pending atomic counter for pending tasks
	pending atomic.Int64
	// running if scheduler is running
	running atomic.Bool
	// closing if scheduler is closing
	closing atomic.Bool
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

func (s *scheduler[T]) Run(ctx context.Context) (err error) {
	if !s.running.CompareAndSwap(false, true) {
		return codex.New(ERROR__SCHEDULER_RERUN)
	}

	s.syn = synapse.NewSynapse(
		ctx,
		synapse.WithBeforeCloseFunc(func(ctx context.Context) {
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
						s.cond.Broadcast()
						time.Sleep(10 * time.Millisecond)
					}
				}
			}()
		}),
		synapse.WithAfterCloseFunc(func(cause error) error {
			if s.exitCbCalled.CompareAndSwap(false, true) {
				if s.scheExitCallback != nil {
					s.scheExitCallback(cause)
				}
			}
			return nil
		}),
		synapse.WithShutdownTimeout(s.closeTimeout),
	)

	defer func() {
		if err != nil {
			s.syn.Cancel(err)
			<-s.syn.Done()
		}
	}()

	for range s.parallel {
		if err = s.syn.Spawn(s.run); err == nil {
			continue
		}
		return
	}
	return nil
}

func (s *scheduler[T]) run(ctx context.Context) {
	for {
		s.cond.L.Lock()
		// avoid spurious waking up
		for s.tasks.Len() == 0 && !s.syn.Canceled() {
			s.cond.Wait()
		}
		if s.syn.Canceled() {
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

func (s *scheduler[T]) Pending() int {
	if s.running.Load() {
		return int(s.pending.Load())
	}
	return 0
}

func (s *scheduler[T]) Close() error {
	if s.syn != nil {
		s.syn.Cancel(codex.New(ERROR__SCHEDULER_CANCELED))
		err := s.syn.Err()
		if codex.IsCode(err, synapse.ERROR__SYNAPSE_CLOSE_TIMEOUT) {
			return err
		}
	}
	return nil
}
