package fleet

import (
	"context"
	"testing"
	"time"
)

// A runtime config change only affects undispatched work: already
// dispatched actions are never rewritten, and the final conclusion
// still matches the unchanged baseline because action content and
// latency depend only on (seed, index).
func TestConfigChangeAffectsOnlyPending(t *testing.T) {
	cfg := testConfig()

	// Part 1: synchronous check that dispatched actions are stable.
	c, err := NewCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor("e", 0); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.dispatchLocked()
	snapshot := make(map[int]Action)
	for i, s := range c.state {
		if s == idxDispatched {
			snapshot[i] = c.actions[i]
		}
	}
	if len(snapshot) == 0 {
		c.mu.Unlock()
		t.Fatalf("nothing dispatched")
	}
	c.UpdateConfigLockedForTest()
	for i, want := range snapshot {
		if c.actions[i] != want {
			t.Fatalf("dispatched action %d rewritten: %+v -> %+v", i, want, c.actions[i])
		}
		if c.shardOf[i] != want.Shard {
			t.Fatalf("dispatched index %d re-sharded", i)
		}
	}
	c.mu.Unlock()

	// Part 2: end-to-end, mid-run shard-count and batch-size change.
	base := runFleet(t, cfg, func(cc *Coordinator) {
		if err := cc.AddExecutor("solo", time.Millisecond); err != nil {
			t.Fatal(err)
		}
	})
	c2, err := NewCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"x", "y"} {
		if err := c2.AddExecutor(id, time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan *Report, 1)
	go func() {
		rep, _ := c2.Run(ctx)
		done <- rep
	}()
	time.Sleep(50 * time.Millisecond)
	c2.UpdateConfig(func(cfg *Config) {
		cfg.ShardCount = 5
		cfg.BatchSize = 20
	})
	rep := <-done
	if rep.Completed != cfg.TotalActions {
		t.Fatalf("completed %d of %d after config change", rep.Completed, cfg.TotalActions)
	}
	if rep.P50 != base.P50 || rep.P90 != base.P90 || rep.P99 != base.P99 ||
		rep.Throughput != base.Throughput || rep.TotalBytes != base.TotalBytes {
		t.Fatalf("conclusion diverged after config change")
	}
}

// UpdateConfigLockedForTest exercises the config-change path while the
// caller holds the coordinator lock.
func (c *Coordinator) UpdateConfigLockedForTest() {
	old := c.cfg.ShardCount
	c.cfg.ShardCount = old + 3
	pending := make([]int, 0)
	for i := 0; i < c.cfg.TotalActions; i++ {
		if c.state[i] == idxPending {
			pending = append(pending, i)
		}
	}
	c.queues = make([][]int, c.cfg.ShardCount)
	for _, i := range pending {
		s := shardOf(c.cfg.Seed, c.cfg.ShardCount, i)
		c.queues[s] = append(c.queues[s], i)
	}
	c.owner = make([]int, c.cfg.ShardCount)
	c.rebalanceLocked()
}
