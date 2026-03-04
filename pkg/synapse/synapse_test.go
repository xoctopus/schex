package synapse_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xoctopus/x/misc/must"
	. "github.com/xoctopus/x/testx"

	. "github.com/xoctopus/schex/pkg/synapse"
)

type MockWorker func(ctx context.Context, cost time.Duration, id int)

func NewAndSpawn(ctx context.Context, worker MockWorker, cost, shutdown time.Duration) Synapse {
	// 1. Initialize synapse with a 2-second shutdown timeout
	options := []OptionApplier{
		WithBeforeCloseFunc(func(_ context.Context) {
			fmt.Println("Before done")
		}),
		WithAfterCloseFunc(func(err error) error {
			fmt.Printf("After: %v\n", err)
			return nil
		}),
	}
	if shutdown > 0 {
		options = append(options, WithShutdownTimeout(shutdown*unit))
	}
	s := NewSynapse(ctx, options...)

	// 2. Spawn workers
	for i := 1; i <= 3; i++ {
		workerID := i
		must.NoError(
			s.Spawn(func(ctx context.Context) {
				worker(ctx, cost*unit, workerID)
			}),
		)
	}
	return s
}

var (
	unit = time.Millisecond * 100

	// worker1 do task controlled by context
	worker1 = func(ctx context.Context, cost time.Duration, wid int) {
		select {
		case <-time.After(cost):
			fmt.Printf("Worker %d finished task\n", wid)
		case <-ctx.Done():
			fmt.Printf("Worker %d canceled\n", wid)
		}
	}

	// worker1 do task without context controlling
	worker2 = func(ctx context.Context, cost time.Duration, wid int) {
		select {
		case <-time.After(cost):
			fmt.Printf("Worker %d finished task\n", wid)
		}
	}
)

func ExampleSynapse() {
	outside, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	ctx, cancel2 := context.WithCancel(outside)
	defer cancel2()

	fmt.Println("==> worker was scheduled before synapse.Cancel")
	s := NewAndSpawn(ctx, worker1, 1, 1)
	time.Sleep(3 * unit)
	s.Cancel(nil)
	fmt.Printf("Closed: %v\n\n", s.Err())

	fmt.Println("==> worker was not scheduled before synapse.Cancel")
	s = NewAndSpawn(ctx, worker1, 2, 1)
	time.Sleep(1 * unit)
	s.Cancel(nil)
	fmt.Printf("Closed: %v\n\n", s.Err())

	fmt.Println("==> without shutdown timeout")
	s = NewAndSpawn(ctx, worker2, 5, 0)
	time.Sleep(8 * unit)
	s.Cancel(nil)
	fmt.Printf("Closed: %v\n\n", s.Err())

	fmt.Println("==> synapse.Close timeout")
	s = NewAndSpawn(ctx, worker2, 5, 1)
	s.Cancel(errors.New("cause"))
	fmt.Printf("Closed: %v\n\n", s.Err())
	time.Sleep(6 * unit)

	fmt.Println("==> shutdown triggered by outside synapse.Cancel")
	s = NewAndSpawn(ctx, worker2, 5, 1)
	cancel(errors.New("outside"))
	<-s.Done()
	fmt.Printf("Closed: %v\n\n", s.Err())
	time.Sleep(6 * unit) // wait
	_ = 1

	// Unordered Output:
	// ==> worker was scheduled before synapse.Cancel
	// Worker 1 finished task
	// Worker 2 finished task
	// Worker 3 finished task
	// Before done
	// After: <nil>
	// Closed: <nil>
	//
	// ==> worker was not scheduled before synapse.Cancel
	// Before done
	// Worker 1 canceled
	// Worker 2 canceled
	// Worker 3 canceled
	// After: <nil>
	// Closed: <nil>
	//
	// ==> without shutdown timeout
	// Worker 1 finished task
	// Worker 2 finished task
	// Worker 3 finished task
	// Before done
	// After: <nil>
	// Closed: <nil>
	//
	// ==> synapse.Close timeout
	// Before done
	// After: [synapse.Error:2] SYNAPSE_CLOSE_TIMEOUT
	// cause
	// Closed: [synapse.Error:2] SYNAPSE_CLOSE_TIMEOUT
	// cause
	//
	// Worker 1 finished task
	// Worker 2 finished task
	// Worker 3 finished task
	// ==> shutdown triggered by outside synapse.Cancel
	// Before done
	// After: [synapse.Error:2] SYNAPSE_CLOSE_TIMEOUT
	// outside
	// Closed: [synapse.Error:2] SYNAPSE_CLOSE_TIMEOUT
	// outside
	//
	// Worker 1 finished task
	// Worker 2 finished task
	// Worker 3 finished task
}

func TestNewSynapse(t *testing.T) {
	var (
		ctx    = context.Background()
		cancel context.CancelFunc
	)
	ctx = context.WithValue(ctx, "key", 100)
	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	s := NewSynapse(ctx)
	Expect(t, s.Parent(), Equal(ctx))
	Expect(t, s.Children(), NotEqual(ctx))

	Expect(t, s.Value("key"), Equal(ctx.Value("key")))
	Expect(t, s.Value("non"), Equal(ctx.Value("non")))

	ts1, ok1 := s.Deadline()
	ts2, ok2 := ctx.Deadline()
	Expect(t, ts1, Equal(ts2))
	Expect(t, ok1, Equal(ok2))

	Expect(t, s.Canceled(), BeFalse())
	s.Cancel(nil)
	<-s.Done()
	Expect(t, s.Canceled(), BeTrue())
	Expect(t, s.Spawn(func(ctx context.Context) {}), IsCodeError(ERROR__SYNAPSE_CLOSED))
}
