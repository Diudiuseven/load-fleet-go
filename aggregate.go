package fleet

import (
	"sort"
	"sync"
	"time"
)

// Aggregator collects samples from all executors, deduplicates them by
// action index, and computes the final machine-readable report. Samples
// are aligned onto a single time axis by their scheduled offset, so
// results merge seamlessly across re-shardings.
type Aggregator struct {
	mu          sync.Mutex
	seen        map[int]struct{}
	latencies   []time.Duration
	dups        int
	perExecutor map[string]int
	buckets     map[int64]int // bucket index (At/width) -> count
	bucketWidth time.Duration
}

// NewAggregator creates an empty aggregator. bucketWidth is the width of
// the timeline buckets used to align samples onto one time axis.
func NewAggregator(bucketWidth time.Duration) *Aggregator {
	if bucketWidth <= 0 {
		bucketWidth = defaultActionInterval
	}
	return &Aggregator{
		seen:        make(map[int]struct{}),
		perExecutor: make(map[string]int),
		buckets:     make(map[int64]int),
		bucketWidth: bucketWidth,
	}
}

// Add records a sample. It returns true when the sample was a duplicate
// (an executor re-sent it or a reassigned executor re-ran it); duplicates
// are never counted twice.
func (a *Aggregator) Add(s Sample) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.seen[s.ActionIndex]; ok {
		a.dups++
		return true
	}
	a.seen[s.ActionIndex] = struct{}{}
	a.latencies = append(a.latencies, s.Latency)
	a.perExecutor[s.ExecutorID]++
	a.buckets[int64(s.At/a.bucketWidth)]++
	return false
}

// Completed returns the number of distinct actions completed.
func (a *Aggregator) Completed() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.seen)
}

// Duplicates returns the number of dropped duplicate samples.
func (a *Aggregator) Duplicates() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.dups
}

// SamplesPerExecutor returns a copy of the per-executor sample counts.
func (a *Aggregator) SamplesPerExecutor() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]int, len(a.perExecutor))
	for k, v := range a.perExecutor {
		out[k] = v
	}
	return out
}

// percentile returns the p-th percentile (p in [0,100]) of sorted values
// using the nearest-rank method.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(p / 100 * float64(len(sorted)))
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// summary computes percentiles, throughput and the aligned timeline.
// wall is the wall-clock duration of the run.
func (a *Aggregator) summary(wall time.Duration) (completed, dups int, pcts map[string]time.Duration, throughput float64, timeline []TimeBucket) {
	a.mu.Lock()
	defer a.mu.Unlock()
	sorted := make([]time.Duration, len(a.latencies))
	copy(sorted, a.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pcts = map[string]time.Duration{
		"p50": percentile(sorted, 50),
		"p90": percentile(sorted, 90),
		"p99": percentile(sorted, 99),
	}
	if wall > 0 {
		throughput = float64(len(a.seen)) / wall.Seconds()
	}
	var maxBucket int64
	for b := range a.buckets {
		if b > maxBucket {
			maxBucket = b
		}
	}
	for i := int64(0); i <= maxBucket; i++ {
		if n := a.buckets[i]; n > 0 {
			timeline = append(timeline, TimeBucket{
				Offset: time.Duration(i) * a.bucketWidth,
				Count:  n,
			})
		}
	}
	return len(a.seen), a.dups, pcts, throughput, timeline
}
