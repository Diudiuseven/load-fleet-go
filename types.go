// Package fleet implements an in-memory, multi-executor load generation
// and result aggregation system. A single load curve is sharded across
// multiple executors that run in the local process; coordination and
// dispatch happen only over in-memory channels.
package fleet

import "time"

// ShardStrategy decides how actions of the curve map to virtual shards
// (slots). The mapping action->slot is fixed for a given config and seed,
// independent of how many executors are alive.
type ShardStrategy int

const (
	// ShardModulo assigns action i to slot i%ShardCount.
	ShardModulo ShardStrategy = iota
	// ShardHash assigns action i to slot hash(seed, i)%ShardCount.
	ShardHash
	// ShardRange assigns contiguous index ranges to slots.
	ShardRange
)

// Config controls a fleet run. Seed, TotalActions, ShardCount and
// ShardStrategy are immutable once a run starts; the remaining fields may
// be changed at runtime and only affect work not yet dispatched.
type Config struct {
	Seed              int64
	TotalActions      int
	ShardCount        int // number of virtual shards (slots)
	ShardStrategy     ShardStrategy
	MaxExecutors      int
	BatchSize         int // actions per dispatched batch
	MaxInflight       int // max unacked batches per executor (backpressure)
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	// ActionInterval is the nominal spacing between consecutive actions
	// on the curve's time axis. Zero means a default of 1ms.
	ActionInterval time.Duration
}

// Action is a single unit of load. Its content is a deterministic
// function of the run seed and its index.
type Action struct {
	Index   int
	Slot    int
	At      time.Duration // scheduled offset from run start
	Payload uint64
}

// Sample is the result of executing one Action, reported by an executor.
type Sample struct {
	ActionIndex int
	Slot        int
	ExecutorID  string
	At          time.Duration
	Latency     time.Duration
}

// ExecutorState distinguishes how an executor ended up, so that lost or
// timed-out executors are recorded distinctly in the final report.
type ExecutorState string

const (
	ExecutorActive    ExecutorState = "active"
	ExecutorCompleted ExecutorState = "completed"
	ExecutorLost      ExecutorState = "lost"
	ExecutorTimeout   ExecutorState = "timeout"
	ExecutorCanceled  ExecutorState = "canceled"
)

// ExecutorReport is the per-executor, machine-readable summary.
type ExecutorReport struct {
	ID         string        `json:"id"`
	State      ExecutorState `json:"state"`
	Slots      []int         `json:"slots"`
	Samples    int           `json:"samples"`
	Dispatched int           `json:"dispatched"`
	Committed  int           `json:"committed"`
}

// TimeBucket aligns samples from all executors onto one time axis.
type TimeBucket struct {
	Offset time.Duration `json:"offset"`
	Count  int           `json:"count"`
}

// Report is the final, machine-readable run conclusion.
type Report struct {
	TotalActions int                      `json:"total_actions"`
	Completed    int                      `json:"completed"`
	Duplicates   int                      `json:"duplicates"`
	Throughput   float64                  `json:"throughput"`
	Percentiles  map[string]time.Duration `json:"percentiles"`
	Executors    []ExecutorReport         `json:"executors"`
	Timeline     []TimeBucket             `json:"timeline"`
	Canceled     bool                     `json:"canceled"`
}
