package fleet

import (
	"context"
	"testing"
	"time"
)

// A slow executor that misses heartbeats must be marked lost, its
// shards taken over by survivors, and its late reports deduplicated.
// The final conclusion must equal the single-executor baseline.
func TestExecutorFailureTakeover(t *testing.T) {
	cfg := testConfig()
	cfg.TotalActions = 4000
	cfg.HeartbeatTimeout = 120 * time.Millisecond

	base := runFleet(t, cfg, func(c *Coordinator) {
		if err := c.AddExecutor("solo", 2*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	})

	c, err := NewCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// flaky processes each batch slower than the heartbeat timeout, so
	// it will be declared lost while still working; its late reports
	// then arrive after its shards were taken over.
	if err := c.AddExecutor("flaky", 2*cfg.HeartbeatTimeout); err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor("steady", 2*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rep, err := c.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if rep.Completed != cfg.TotalActions {
		t.Fatalf("completed %d of %d: gap left after takeover", rep.Completed, cfg.TotalActions)
	}
	if rep.DuplicatesDropped == 0 {
		t.Fatalf("expected late duplicate reports to be dropped")
	}
	if rep.P50 != base.P50 || rep.P90 != base.P90 || rep.P99 != base.P99 {
		t.Fatalf("percentiles diverge: got %v/%v/%v want %v/%v/%v",
			rep.P50, rep.P90, rep.P99, base.P50, base.P90, base.P99)
	}
	if rep.Throughput != base.Throughput || rep.TotalBytes != base.TotalBytes {
		t.Fatalf("cumulative stats diverge")
	}
	var flakyRep *ExecutorReport
	for i := range rep.Executors {
		if rep.Executors[i].ID == "flaky" {
			flakyRep = &rep.Executors[i]
		}
	}
	if flakyRep == nil || flakyRep.Status != StatusLost {
		t.Fatalf("flaky executor not recorded as lost: %+v", flakyRep)
	}
	// The coordinator must expose the executor's lag.
	if flakyRep.Lag < cfg.HeartbeatTimeout {
		t.Fatalf("lag %v not observable (>= %v expected)", flakyRep.Lag, cfg.HeartbeatTimeout)
	}
}

// A stuck executor that never recovers must not wedge the run: its
// in-flight work is requeued and completed by others.
func TestStuckExecutorIsolated(t *testing.T) {
	cfg := testConfig()
	cfg.TotalActions = 800
	cfg.HeartbeatTimeout = 100 * time.Millisecond

	c, err := NewCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor("stuck", 0); err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor("worker", 0); err != nil {
		t.Fatal(err)
	}
	c.SetExecutorStuck("stuck", true)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rep, err := c.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Completed != cfg.TotalActions {
		t.Fatalf("completed %d of %d", rep.Completed, cfg.TotalActions)
	}
	status := map[string]Status{}
	for _, er := range rep.Executors {
		status[er.ID] = er.Status
	}
	if status["stuck"] != StatusLost {
		t.Fatalf("stuck executor status = %v, want lost", status["stuck"])
	}
}

// Direct duplicate injection: re-sent report batches must be dropped by
// the aggregator and never counted twice.
func TestDuplicateReportsDropped(t *testing.T) {
	cfg := testConfig()
	c, err := NewCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor("e", 0); err != nil {
		t.Fatal(err)
	}
	rb := ReportBatch{
		ExecutorID: "e",
		Samples: []Sample{
			{ActionIndex: 0, Shard: shardOf(cfg.Seed, cfg.ShardCount, 0), ExecutorID: "e", Latency: time.Millisecond},
			{ActionIndex: 1, Shard: shardOf(cfg.Seed, cfg.ShardCount, 1), ExecutorID: "e", Latency: time.Millisecond},
		},
	}
	c.mu.Lock()
	c.handleReportLocked(rb)
	c.handleReportLocked(rb) // re-sent
	c.handleReportLocked(rb) // and again
	got := c.agg.count()
	dropped := c.agg.dropped
	c.mu.Unlock()
	if got != 2 {
		t.Fatalf("counted %d samples, want 2", got)
	}
	if dropped != 4 {
		t.Fatalf("dropped %d duplicates, want 4", dropped)
	}
}
