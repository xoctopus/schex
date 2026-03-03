/*
Package synapse provides a high-level abstraction for structured concurrency
and goroutine lifecycle management.

The core component, synapse, acts as a controlled container for asynchronous
workers.
It ensures that all spawned goroutines are properly tracked via a sync.WaitGroup
and responsive to a unified lifecycle boundary, facilitating graceful shutdowns.

Key Features:
  - Wrapping context: Separates the Parent context (metadata/inherited signals)
    from the Dispatched context (internal lifecycle control).
  - Go 1.24 Integration: Leverages sync.WaitGroup.Go for automatic task tracking
    and standardized panic propagation.
  - Shutdown Orchestration: Provides a thread-safe Cancel mechanism with optional
    timeout protection to prevent blocking the main process indefinitely.

Usage Example:

	s := syn.NewSynapse(ctx,
	    syn.WithCancelCause(errors.New("service stopped")),
	    syn.WithShutdownTimeout(5 * time.Second),
	)

	// Spawn a managed worker
	s.Spawn(func(ctx context.Context) {
	    // Worker automatically tracked by WaitGroup
	    doWork(ctx)
	})

	// Gracefully stop all workers and wait for completion
	if err := s.Cancel(nil); err != nil {
	    log.Printf("Shutdown failed: %v", err)
	}
*/
package synapse
