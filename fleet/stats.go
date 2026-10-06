package fleet

import (
	"sort"
	"time"
)

// aggregator collects samples from all executors, deduplicating by
// action index so re-sent or late reports never count twice.
type aggregator struct {
	seen     map[int]struct{}
	lat      []time.Duration
	bytes    uint64
	dropped  int // duplicate samples ignored
	maxSched time.Duration
}

func newAggregator() *aggregator {
	return &aggregator{seen: make(map[int]struct{})}
}

// add records a sample; it returns false when the action was already
// accounted for (duplicate or late report).
func (a *aggregator) add(s Sample, sched time.Duration) bool {
	if _, ok := a.seen[s.ActionIndex]; ok {
		a.dropped++
		return false
	}
	a.seen[s.ActionIndex] = struct{}{}
	a.lat = append(a.lat, s.Latency)
	a.bytes += s.Bytes
	if sched > a.maxSched {
		a.maxSched = sched
	}
	return true
}

func (a *aggregator) count() int { return len(a.lat) }

// percentile returns the p-th percentile (0..100) of recorded latencies
// using the nearest-rank method; 0 for empty input.
func (a *aggregator) percentile(p float64) time.Duration {
	if len(a.lat) == 0 {
		return 0
	}
	s := make([]time.Duration, len(a.lat))
	copy(s, a.lat)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	rank := int(p / 100 * float64(len(s)))
	if rank >= len(s) {
		rank = len(s) - 1
	}
	return s[rank]
}

// throughput is accepted actions per second over the scheduled span.
func (a *aggregator) throughput() float64 {
	if a.maxSched <= 0 {
		return float64(len(a.lat))
	}
	return float64(len(a.lat)) / a.maxSched.Seconds()
}
