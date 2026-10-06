package fleet

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// idxState is the lifecycle state of one curve position.
type idxState uint8

const (
	idxPending idxState = iota
	idxDispatched
	idxCommitted
)

// execState is the coordinator's view of one executor.
type execState struct {
	ex          *Executor
	status      Status
	inFlight    int          // dispatched, not yet reported batches
	inFlightIdx map[int]bool // indices inside those batches
	inboxCap    int          // fixed capacity of the executor inbox
	lastSeen    time.Time
	samples     int64 // accepted samples reported by this executor
	cancel      context.CancelFunc
}

// Coordinator owns the run: it shards the curve, dispatches batches to
// executors, enforces backpressure, detects failures, reassigns work and
// aggregates deduplicated samples into a final report.
type Coordinator struct {
	mu  sync.Mutex
	cfg Config

	executors map[string]*execState
	order     []string // deterministic executor iteration order

	state   []idxState // per curve position
	shardOf []int      // position -> shard assigned at dispatch time (-1 pending)
	queues  [][]int    // shard -> pending positions (increasing)
	owner   []int      // shard -> index into order, -1 unassigned
	agg     *aggregator
	actions []Action // built lazily at dispatch, never rewritten

	reportCh chan ReportBatch
	now      func() time.Time

	runCtx  context.Context // set when Run starts
	doneCh  chan struct{}
	started bool

	hook func(ev string, idx int, id string) // debug/test instrumentation
}

// NewCoordinator validates the config and prepares the run.
func NewCoordinator(cfg Config) (*Coordinator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	queues := make([][]int, cfg.ShardCount)
	for i := 0; i < cfg.TotalActions; i++ {
		s := shardOf(cfg.Seed, cfg.ShardCount, i)
		queues[s] = append(queues[s], i)
	}
	sh := make([]int, cfg.TotalActions)
	for i := range sh {
		sh[i] = -1
	}
	return &Coordinator{
		cfg:       cfg,
		executors: make(map[string]*execState),
		state:     make([]idxState, cfg.TotalActions),
		shardOf:   sh,
		queues:    queues,
		owner:     make([]int, cfg.ShardCount),
		agg:       newAggregator(),
		actions:   make([]Action, cfg.TotalActions),
		reportCh:  make(chan ReportBatch, 1024),
		now:       time.Now,
		doneCh:    make(chan struct{}),
	}, nil
}

// AddExecutor joins a new executor to the run. It may be called before
// or during Run. delay simulates a slow worker. It returns an error if
// MaxExecutors would be exceeded.
func (c *Coordinator) AddExecutor(id string, delay time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.executors[id]; ok {
		return fmt.Errorf("fleet: executor %q already exists", id)
	}
	if c.liveLocked() >= c.cfg.MaxExecutors {
		return fmt.Errorf("fleet: MaxExecutors %d reached", c.cfg.MaxExecutors)
	}
	st := &execState{
		ex:          newExecutor(id, c.cfg, c.reportCh, delay),
		status:      StatusActive,
		inFlightIdx: make(map[int]bool),
		inboxCap:    c.cfg.MaxInFlight,
		lastSeen:    c.now(),
	}
	c.executors[id] = st
	c.order = append(c.order, id)
	sort.Strings(c.order)
	c.rebalanceLocked()
	if c.started && c.runCtx != nil {
		ectx, ecancel := context.WithCancel(c.runCtx)
		st.cancel = ecancel
		go st.ex.run(ectx)
	}
	return nil
}

// RemoveExecutor gracefully removes an executor: its pending shards are
// reassigned and its in-flight indices are requeued so no work is lost.
func (c *Coordinator) RemoveExecutor(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(id, StatusLeft)
}

// SetExecutorStuck simulates a hung executor (failure injection).
func (c *Coordinator) SetExecutorStuck(id string, stuck bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st, ok := c.executors[id]; ok {
		st.ex.setStuck(stuck)
	}
}

// ExecutorCommitted exposes an executor's own progress view (for tests
// and observability).
func (c *Coordinator) ExecutorCommitted(id string) map[int]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st, ok := c.executors[id]; ok {
		return st.ex.Committed()
	}
	return nil
}

func (c *Coordinator) liveLocked() int {
	n := 0
	for _, st := range c.executors {
		if st.status == StatusActive {
			n++
		}
	}
	return n
}

// removeLocked tears down an executor, requeues its in-flight indices
// and hands its shards to the survivors.
func (c *Coordinator) removeLocked(id string, status Status) {
	st, ok := c.executors[id]
	if !ok || st.status != StatusActive {
		return
	}
	st.status = status
	// Do not cancel the executor context here: a merely partitioned or
	// slow executor may still deliver late reports, which are safely
	// deduplicated. Closing the inbox lets it drain and exit; the
	// run-level context guarantees final convergence.
	close(st.ex.inbox)
	// Requeue in-flight, uncommitted indices at the head of their shard
	// queues so they are redispatched first.
	for idx := range st.inFlightIdx {
		if c.state[idx] == idxDispatched {
			if c.hook != nil {
				c.hook("requeue", idx, id)
			}
			c.state[idx] = idxPending
			s := c.shardOf[idx]
			c.queues[s] = append([]int{idx}, c.queues[s]...)
		}
	}
	st.inFlightIdx = make(map[int]bool)
	st.inFlight = 0
	c.rebalanceLocked()
}

// rebalanceLocked assigns shards to live executors as evenly as
// possible. In-flight batches are unaffected: their indices stay with
// the reporting executor and are deduplicated on arrival.
func (c *Coordinator) rebalanceLocked() {
	live := make([]int, 0, len(c.order))
	for i, id := range c.order {
		if c.executors[id].status == StatusActive {
			live = append(live, i)
		}
	}
	for s := range c.owner {
		c.owner[s] = -1
	}
	if len(live) == 0 {
		return
	}
	for s := 0; s < len(c.queues); s++ {
		c.owner[s] = live[s%len(live)]
	}
}

// ownerOf returns the executor id owning shard s, or "".
func (c *Coordinator) ownerOf(s int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.owner[s] < 0 {
		return ""
	}
	return c.order[c.owner[s]]
}
