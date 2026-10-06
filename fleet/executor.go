package fleet

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Executor is an in-process load executor. It receives batches over a
// buffered channel (which provides natural backpressure), simulates
// applying each action, and reports samples plus committed progress back
// to the coordinator. All coordination is in-memory; no external
// services are involved.
type Executor struct {
	id     string
	cfg    Config
	inbox  chan Batch
	report chan<- ReportBatch

	// delay simulates a slow executor: each batch takes at least this
	// long to process.
	delay time.Duration
	// stuck, when set, makes the executor stop consuming and stop
	// heartbeating, simulating a hung or crashed worker.
	stuck atomic.Bool

	mu        sync.Mutex
	committed map[int]int // shard -> contiguous committed count
	done      chan struct{}
}

func newExecutor(id string, cfg Config, report chan<- ReportBatch, delay time.Duration) *Executor {
	return &Executor{
		id:        id,
		cfg:       cfg,
		inbox:     make(chan Batch, cfg.MaxInFlight),
		report:    report,
		delay:     delay,
		committed: make(map[int]int),
		done:      make(chan struct{}),
	}
}

// ID returns the executor identifier.
func (e *Executor) ID() string { return e.id }

// setStuck simulates a hung executor (used by tests and failure drills).
func (e *Executor) setStuck(v bool) { e.stuck.Store(v) }

// Committed returns a snapshot of per-shard committed progress.
func (e *Executor) Committed() map[int]int {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[int]int, len(e.committed))
	for k, v := range e.committed {
		out[k] = v
	}
	return out
}

// simulatedLatency derives the response latency of action i from the
// seed alone, so conclusions are reproducible for a fixed seed and
// config regardless of executor count.
func simulatedLatency(seed int64, i int) time.Duration {
	h := mix64(uint64(seed)^0x1a7e, uint64(i))
	return time.Duration(1+h%50) * time.Millisecond
}

// run is the executor main loop. It exits on context cancellation or
// when the inbox is closed, releasing all resources.
func (e *Executor) run(ctx context.Context) {
	defer close(e.done)
	hb := time.NewTicker(e.cfg.HeartbeatTimeout / 3)
	defer hb.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-e.inbox:
			if !ok {
				return
			}
			if e.stuck.Load() {
				continue
			}
			e.process(ctx, b)
		case <-hb.C:
			if e.stuck.Load() {
				continue
			}
			e.send(ctx, ReportBatch{ExecutorID: e.id, Committed: e.Committed()})
		}
	}
}

func (e *Executor) process(ctx context.Context, b Batch) {
	if e.delay > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.delay):
		}
	}
	rb := ReportBatch{ExecutorID: e.id, Committed: make(map[int]int, 1)}
	for _, a := range b.Actions {
		rb.Samples = append(rb.Samples, Sample{
			ActionIndex: a.Index,
			Shard:       a.Shard,
			ExecutorID:  e.id,
			Latency:     simulatedLatency(e.cfg.Seed, a.Index),
			Bytes:       a.Payload,
		})
	}
	e.mu.Lock()
	e.committed[b.Shard] += len(b.Actions)
	rb.Committed[b.Shard] = e.committed[b.Shard]
	e.mu.Unlock()
	e.send(ctx, rb)
}

func (e *Executor) send(ctx context.Context, rb ReportBatch) {
	select {
	case e.report <- rb:
	case <-ctx.Done():
	}
}
