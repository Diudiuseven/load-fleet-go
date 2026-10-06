package fleet

import (
	"fmt"
	"time"
)

// Config controls a fleet run. A Config change at runtime only affects
// batches that have not been dispatched yet; dispatched or completed
// work is never rewritten.
type Config struct {
	Seed             int64 // fixes all randomness; same seed => same run
	TotalActions     int   // length of the load curve
	ShardCount       int   // fixed shard space; independent of executor count
	MaxExecutors     int   // upper bound on concurrent executors
	BatchSize        int   // actions per dispatched batch
	MaxInFlight      int   // per-executor in-flight batch limit (backpressure)
	HeartbeatTimeout time.Duration
	// Curve shapes the schedule. Action i lands at
	// Duration*i/TotalActions plus a deterministic per-index jitter.
	Duration   time.Duration
	JitterFrac float64 // 0..1, fraction of the mean spacing used as jitter
}

// DefaultConfig returns a sane starting point.
func DefaultConfig() Config {
	return Config{
		Seed:             1,
		TotalActions:     1000,
		ShardCount:       16,
		MaxExecutors:     8,
		BatchSize:        10,
		MaxInFlight:      4,
		HeartbeatTimeout: 2 * time.Second,
		Duration:         time.Minute,
		JitterFrac:       0.5,
	}
}

// Validate checks internal consistency.
func (c Config) Validate() error {
	if c.TotalActions <= 0 {
		return fmt.Errorf("fleet: TotalActions must be > 0")
	}
	if c.ShardCount <= 0 {
		return fmt.Errorf("fleet: ShardCount must be > 0")
	}
	if c.MaxExecutors <= 0 {
		return fmt.Errorf("fleet: MaxExecutors must be > 0")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("fleet: BatchSize must be > 0")
	}
	if c.MaxInFlight <= 0 {
		return fmt.Errorf("fleet: MaxInFlight must be > 0")
	}
	if c.HeartbeatTimeout <= 0 {
		return fmt.Errorf("fleet: HeartbeatTimeout must be > 0")
	}
	if c.Duration <= 0 {
		return fmt.Errorf("fleet: Duration must be > 0")
	}
	if c.JitterFrac < 0 || c.JitterFrac > 1 {
		return fmt.Errorf("fleet: JitterFrac must be in [0,1]")
	}
	return nil
}
