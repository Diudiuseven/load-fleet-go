package fleet

import (
	"context"
	"testing"
	"time"
)

func testConfig() Config {
	c := DefaultConfig()
	c.TotalActions = 600
	c.ShardCount = 8
	c.BatchSize = 7
	c.MaxInFlight = 3
	c.HeartbeatTimeout = 300 * time.Millisecond
	c.Duration = 10 * time.Second
	return c
}

// runFleet runs a coordinator to completion with executors added as
// specified and returns the report.
func runFleet(t *testing.T, cfg Config, add func(c *Coordinator)) *Report {
	t.Helper()
	c, err := NewCoordinator(cfg)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	if add != nil {
		add(c)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rep, err := c.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return rep
}

// Shards must form a complete, non-overlapping partition of the curve,
// independent of executor count.
func TestShardPartitionComplete(t *testing.T) {
	cfg := testConfig()
	for _, shards := range []int{1, 3, 8, 16} {
		cfg.ShardCount = shards
		seen := make(map[int]int)
		for s := 0; s < shards; s++ {
			for _, i := range shardIndices(cfg, s) {
				if shardOf(cfg.Seed, shards, i) != s {
					t.Fatalf("shardIndices(%d) contains %d owned by %d", s, i, shardOf(cfg.Seed, shards, i))
				}
				seen[i]++
			}
		}
		if len(seen) != cfg.TotalActions {
			t.Fatalf("shards=%d: covered %d of %d indices", shards, len(seen), cfg.TotalActions)
		}
		for i, n := range seen {
			if n != 1 {
				t.Fatalf("shards=%d: index %d covered %d times", shards, i, n)
			}
		}
	}
}

// Same seed and config: any executor count must yield identical
// reassembled actions and cumulative statistics.
func TestDeterministicAcrossExecutorCounts(t *testing.T) {
	cfg := testConfig()
	var base *Report
	for _, n := range []int{1, 2, 5} {
		rep := runFleet(t, cfg, func(c *Coordinator) {
			for i := 0; i < n; i++ {
				if err := c.AddExecutor(string(rune('a'+i)), 0); err != nil {
					t.Fatalf("AddExecutor: %v", err)
				}
			}
		})
		if rep.Completed != cfg.TotalActions {
			t.Fatalf("executors=%d: completed %d of %d", n, rep.Completed, cfg.TotalActions)
		}
		if rep.DuplicatesDropped != 0 {
			t.Fatalf("executors=%d: unexpected duplicates %d", n, rep.DuplicatesDropped)
		}
		if base == nil {
			base = rep
			continue
		}
		if rep.P50 != base.P50 || rep.P90 != base.P90 || rep.P99 != base.P99 {
			t.Fatalf("executors=%d: percentiles differ: got %v/%v/%v want %v/%v/%v",
				n, rep.P50, rep.P90, rep.P99, base.P50, base.P90, base.P99)
		}
		if rep.Throughput != base.Throughput || rep.TotalBytes != base.TotalBytes {
			t.Fatalf("executors=%d: throughput/bytes differ", n)
		}
	}
}
