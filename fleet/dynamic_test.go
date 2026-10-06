package fleet

import (
	"context"
	"testing"
	"time"
)

// Executors joining and leaving mid-run must not lose or duplicate
// work, and the merged conclusion must match the single-executor run.
func TestDynamicScaleUpDown(t *testing.T) {
	cfg := testConfig()
	cfg.TotalActions = 3000
	const delay = 5 * time.Millisecond
	base := runFleet(t, cfg, func(c *Coordinator) {
		if err := c.AddExecutor("solo", delay); err != nil {
			t.Fatal(err)
		}
	})

	c, err := NewCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"e1", "e2"} {
		if err := c.AddExecutor(id, delay); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	type result struct {
		rep *Report
		err error
	}
	done := make(chan result, 1)
	go func() {
		rep, err := c.Run(ctx)
		done <- result{rep, err}
	}()

	// Scale up and down while running.
	time.Sleep(30 * time.Millisecond)
	if err := c.AddExecutor("e3", 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	c.RemoveExecutor("e1")
	time.Sleep(30 * time.Millisecond)
	if err := c.AddExecutor("e4", 0); err != nil {
		t.Fatal(err)
	}
	c.RemoveExecutor("e2")

	r := <-done
	if r.err != nil {
		t.Fatalf("Run: %v", r.err)
	}
	rep := r.rep
	if rep.Completed != cfg.TotalActions {
		t.Fatalf("completed %d of %d", rep.Completed, cfg.TotalActions)
	}
	if rep.P50 != base.P50 || rep.P90 != base.P90 || rep.P99 != base.P99 {
		t.Fatalf("percentiles diverge from baseline: got %v/%v/%v want %v/%v/%v",
			rep.P50, rep.P90, rep.P99, base.P50, base.P90, base.P99)
	}
	if rep.Throughput != base.Throughput || rep.TotalBytes != base.TotalBytes {
		t.Fatalf("cumulative stats diverge from baseline")
	}
	// Removed executors must be recorded with a distinguishable status.
	status := map[string]Status{}
	for _, er := range rep.Executors {
		status[er.ID] = er.Status
	}
	if status["e1"] != StatusLeft || status["e2"] != StatusLeft {
		t.Fatalf("removed executors not marked left: %v", status)
	}
}

// MaxExecutors must be enforced.
func TestMaxExecutorsEnforced(t *testing.T) {
	cfg := testConfig()
	cfg.MaxExecutors = 2
	c, err := NewCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor("a", 0); err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor("b", 0); err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor("c", 0); err == nil {
		t.Fatal("expected MaxExecutors error")
	}
}
