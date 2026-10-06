package fleet

// mix64 is a splitmix64-style finalizer used for deterministic hashing.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// slotOf maps a global action index to its virtual shard (slot). The
// mapping depends only on (strategy, seed, shardCount, index, total), so
// it is identical no matter how many executors are alive. It is complete
// and non-overlapping: every index maps to exactly one slot in
// [0, shardCount).
func slotOf(strategy ShardStrategy, seed int64, shardCount, index, total int) int {
	if shardCount <= 1 {
		return 0
	}
	switch strategy {
	case ShardHash:
		return int(mix64(uint64(seed)^uint64(index)) % uint64(shardCount))
	case ShardRange:
		// Contiguous, balanced ranges: slot sizes differ by at most 1.
		return index * shardCount / total
	default: // ShardModulo
		return index % shardCount
	}
}

// bucketIndices groups all action indices in [0, total) by slot. The
// union of buckets is exactly [0, total) and buckets are disjoint.
// Indices inside each bucket stay in ascending order, so every slot is a
// deterministic, ordered stream.
func bucketIndices(strategy ShardStrategy, seed int64, shardCount, total int) [][]int {
	if shardCount < 1 {
		shardCount = 1
	}
	buckets := make([][]int, shardCount)
	for i := 0; i < total; i++ {
		s := slotOf(strategy, seed, shardCount, i, total)
		buckets[s] = append(buckets[s], i)
	}
	return buckets
}
