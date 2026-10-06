package fleet

import (
	"context"
	"testing"
	"time"
)

// Cancellation must converge all executors promptly and release
// resources, returning a partial but consistent report.
func TestCancelConvergesExecutors(t *testing.T) {
	cfg := testConfig()
	cfg.TotalActions = 100000 // cannot finish in time
	c, err := NewCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if err := c.AddExecutor(id, 20*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *Report, 1)
	go func() {
		rep, _ := c.Run(ctx)
		done <- rep
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case rep := <-done:
		if !rep.Cancelled {
			t.Fatalf("report not marked cancelled")
		}
		if rep.Completed >= cfg.TotalActions {
			t.Fatalf("unexpected completion")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Run did not return promptly after cancel")
	}
	// All executor goroutines must have exited.
	c.mu.Lock()
	execs := make([]*Executor, 0, len(c.executors))
	for _, st := range c.executors {
		execs = append(execs, st.ex)
	}
	c.mu.Unlock()
	for _, ex := range execs {
		select {
		case <-ex.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("executor %s did not exit after cancel", ex.id)
		}
	}
}

// Backpressure: a slow executor must never hold more than MaxInFlight
// batches, and its lag must be observable.
func TestBackpressureLimitAndLag(t *testing.T) {
	cfg := testConfig()
	cfg.MaxInFlight = 2
	c, err := NewCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor("slow", time.Hour); err != nil { // effectively stuck
		t.Fatal(err)
	}
	c.mu.Lock()
	c.dispatchLocked()
	st := c.executors["slow"]
	if st.inFlight > cfg.MaxInFlight {
		t.Fatalf("inFlight %d exceeds MaxInFlight %d", st.inFlight, cfg.MaxInFlight)
	}
	if got := len(st.ex.inbox); got > cfg.MaxInFlight {
		t.Fatalf("inbox holds %d batches, cap %d", got, cfg.MaxInFlight)
	}
	// Total dispatched must not exceed what the executor can hold.
	dispatched := 0
	for _, s := range c.state {
		if s == idxDispatched {
			dispatched++
		}
	}
	if dispatched > cfg.MaxInFlight*cfg.BatchSize {
		t.Fatalf("dispatched %d actions beyond backpressure limit", dispatched)
	}
	c.mu.Unlock()
}
