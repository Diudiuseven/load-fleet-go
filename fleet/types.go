// Package fleet implements a multi-executor load generation and result
// aggregation engine. Executors are simulated in-process and all
// coordination happens over in-memory channels.
package fleet

import "time"

// Status describes the lifecycle state of an executor as seen by the
// coordinator.
type Status string

const (
	StatusActive  Status = "active"
	StatusLost    Status = "lost"    // heartbeat timeout, shards taken over
	StatusLeft    Status = "left"    // gracefully removed
	StatusDrained Status = "drained" // finished all assigned work
)

// Action is a single unit of load derived deterministically from the
// configured curve and seed. Its content depends only on (Seed, Index),
// never on how many executors exist.
type Action struct {
	Index       int           // global position in the curve
	Shard       int           // owning shard, derived from Index
	ScheduledAt time.Duration // offset on the shared timeline
	Payload     uint64        // simulated request size in bytes
}

// Sample is the result of applying one Action, reported by an executor.
type Sample struct {
	ActionIndex int
	Shard       int
	ExecutorID  string
	Latency     time.Duration // simulated response latency
	Bytes       uint64
}

// Batch is a dispatch unit sent from coordinator to one executor.
type Batch struct {
	Shard   int
	Actions []Action
}

// ReportBatch is a set of samples plus a heartbeat sent back to the
// coordinator.
type ReportBatch struct {
	ExecutorID string
	Samples    []Sample
	// Committed maps shard -> number of contiguously completed actions
	// in that shard's subsequence.
	Committed map[int]int
}
