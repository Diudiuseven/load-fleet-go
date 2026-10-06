package fleet

import "time"

// shard.go derives the deterministic, executor-count-independent
// mapping from curve positions to shards, and the per-index schedule.
//
// All randomness flows from mix64(seed, index): the same seed and config
// always reproduce the same actions, schedule and latencies no matter
// how many executors are used.

// mix64 is a SplitMix64-style finalizer: a cheap, deterministic hash of
// two int64 words.
func mix64(a, b uint64) uint64 {
	z := a + 0x9e3779b97f4a7c15 + b*0xbf58476d1ce4e5b9
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// shardOf returns the shard owning curve position i. It depends only on
// the seed, i and the fixed shard count, so shards form a complete,
// non-overlapping partition that is stable across reconfigurations.
func shardOf(seed int64, shardCount, i int) int {
	return int(mix64(uint64(seed), uint64(i)) % uint64(shardCount))
}

// buildAction reconstructs the action at curve position i.
func buildAction(c Config, i int) Action {
	spacing := c.Duration / time.Duration(c.TotalActions)
	h := mix64(uint64(c.Seed)^0x5eed, uint64(i))
	// Deterministic jitter in [-JitterFrac, +JitterFrac] * spacing.
	jf := (float64(h%1000000)/1000000.0*2 - 1) * c.JitterFrac
	off := time.Duration(i)*spacing + time.Duration(jf*float64(spacing))
	if off < 0 {
		off = 0
	}
	return Action{
		Index:       i,
		Shard:       shardOf(c.Seed, c.ShardCount, i),
		ScheduledAt: off,
		Payload:     128 + h%4096,
	}
}

// shardIndices lists, in increasing order, the curve positions owned by
// shard s. This is the canonical subsequence every owner of the shard
// must walk; progress is a count into this slice.
func shardIndices(c Config, s int) []int {
	var out []int
	for i := 0; i < c.TotalActions; i++ {
		if shardOf(c.Seed, c.ShardCount, i) == s {
			out = append(out, i)
		}
	}
	return out
}
