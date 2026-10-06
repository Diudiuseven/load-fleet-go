package fleet

import (
	"sort"
	"time"
)

// ExecutorReport is the per-executor, machine-readable outcome.
type ExecutorReport struct {
	ID        string        `json:"id"`
	Status    Status        `json:"status"` // active / lost / left / drained
	Shards    []int         `json:"shards"` // shards owned at report time
	Samples   int64         `json:"samples"`
	InFlight  int           `json:"in_flight"`
	Committed map[int]int   `json:"committed"` // executor's own per-shard progress view
	Lag       time.Duration `json:"lag"`       // time since last heartbeat/report
}

// Report is the final aggregated, machine-readable conclusion.
type Report struct {
	TotalActions      int              `json:"total_actions"`
	Completed         int              `json:"completed"`
	DuplicatesDropped int              `json:"duplicates_dropped"`
	P50               time.Duration    `json:"p50"`
	P90               time.Duration    `json:"p90"`
	P99               time.Duration    `json:"p99"`
	Throughput        float64          `json:"throughput"` // actions per scheduled second
	TotalBytes        uint64           `json:"total_bytes"`
	Cancelled         bool             `json:"cancelled"`
	Executors         []ExecutorReport `json:"executors"`
}

// buildReportLocked snapshots the current state into a Report.
func (c *Coordinator) buildReportLocked(cancelled bool) *Report {
	rep := &Report{
		TotalActions:      c.cfg.TotalActions,
		Completed:         c.agg.count(),
		DuplicatesDropped: c.agg.dropped,
		P50:               c.agg.percentile(50),
		P90:               c.agg.percentile(90),
		P99:               c.agg.percentile(99),
		Throughput:        c.agg.throughput(),
		TotalBytes:        c.agg.bytes,
		Cancelled:         cancelled,
	}
	now := c.now()
	for _, id := range c.order {
		st := c.executors[id]
		er := ExecutorReport{
			ID:        id,
			Status:    st.status,
			Samples:   st.samples,
			InFlight:  st.inFlight,
			Committed: st.ex.Committed(),
			Lag:       now.Sub(st.lastSeen),
		}
		for s, o := range c.owner {
			if o >= 0 && c.order[o] == id && st.status == StatusActive {
				er.Shards = append(er.Shards, s)
			}
		}
		sort.Ints(er.Shards)
		if st.status == StatusActive && c.committedLocked() == c.cfg.TotalActions {
			er.Status = StatusDrained
		}
		rep.Executors = append(rep.Executors, er)
	}
	return rep
}

// UpdateConfig applies fn to the live configuration. Changes only
// affect work not yet dispatched: already dispatched or committed
// indices keep the actions and shard assignments computed under the
// old configuration. If ShardCount changes, pending indices are
// re-partitioned under the new count; the union remains complete and
// non-overlapping because dispatched indices are never re-sharded.
func (c *Coordinator) UpdateConfig(fn func(*Config)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	oldShards := c.cfg.ShardCount
	fn(&c.cfg)
	if c.cfg.ShardCount != oldShards {
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
}

// Config returns a snapshot of the current configuration.
func (c *Coordinator) Config() Config {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg
}
