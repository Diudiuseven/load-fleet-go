package fleet

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// ErrTooManyExecutors is returned by AddExecutor when the configured
// MaxExecutors limit is reached.
var ErrTooManyExecutors = errors.New("fleet: max executors reached")

// ErrExecutorExists is returned by AddExecutor for a duplicate id.
var ErrExecutorExists = errors.New("fleet: executor already registered")

// ErrAlreadyStarted is returned by Run when the coordinator already ran.
var ErrAlreadyStarted = errors.New("fleet: coordinator already started")

type cmdKind int

const (
	cmdAdd cmdKind = iota
	cmdRemove
	cmdUpdateCfg
	cmdLag
)

type cmd struct {
	kind  cmdKind
	exec  *Executor
	id    string
	fn    func(*Config)
	reply chan interface{}
}

// slotState tracks one virtual shard: an ordered stream of action
// indices, how many were dispatched, and how many are contiguously
// committed. On reassignment, dispatch resumes at committed, so no
// action is lost and duplicates are absorbed by the aggregator.
type slotState struct {
	indices    []int
	acked      []bool
	committed  int
	dispatched int
	assignedTo string
}

func (s *slotState) complete() bool { return s.committed == len(s.indices) }

type executorHandle struct {
	exec       *Executor
	state      ExecutorState
	slots      map[int]bool // currently assigned slots
	served     map[int]bool // slots ever assigned (for the report)
	inflight   int          // dispatched but unacked actions
	dispatched int          // total actions ever dispatched to it
	lastSeen   time.Time
	graceful   bool // exit is expected (removed or run finished)
}

// Coordinator shards the curve, dispatches batches to executors, tracks
// heartbeats and backpressure, handles membership changes and failure
// takeover, and aggregates the final result. All communication uses
// in-memory channels only.
type Coordinator struct {
	mu      sync.Mutex
	cfg     Config
	curve   *Curve
	slots   []*slotState
	posOf   []int // action index -> position within its slot stream
	agg     *Aggregator
	started bool
	done    chan struct{}

	reports    chan Sample
	heartbeats chan Heartbeat
	exited     chan string
	cmds       chan cmd

	executors map[string]*executorHandle
	order     []string // registration order, for deterministic reports

	runCtx    context.Context
	startTime time.Time
}

// NewCoordinator creates a coordinator for cfg, filling in defaults.
func NewCoordinator(cfg Config) *Coordinator {
	if cfg.ShardCount <= 0 {
		cfg.ShardCount = 16
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 8
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 4
	}
	if cfg.MaxExecutors <= 0 {
		cfg.MaxExecutors = 64
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 500 * time.Millisecond
	}
	curve := NewCurve(cfg)
	buckets := bucketIndices(cfg.ShardStrategy, cfg.Seed, cfg.ShardCount, cfg.TotalActions)
	slots := make([]*slotState, len(buckets))
	posOf := make([]int, cfg.TotalActions)
	for s, idxs := range buckets {
		st := &slotState{indices: idxs, acked: make([]bool, len(idxs))}
		slots[s] = st
		for pos, idx := range idxs {
			posOf[idx] = pos
		}
	}
	return &Coordinator{
		cfg:        cfg,
		curve:      curve,
		slots:      slots,
		posOf:      posOf,
		agg:        NewAggregator(cfg.ActionInterval),
		reports:    make(chan Sample, 4096),
		heartbeats: make(chan Heartbeat, 256),
		exited:     make(chan string, 64),
		cmds:       make(chan cmd, 64),
		done:       make(chan struct{}),
		executors:  make(map[string]*executorHandle),
	}
}

// Aggregator exposes the live aggregator for inspection.
func (c *Coordinator) Aggregator() *Aggregator { return c.agg }

// AddExecutor registers an executor, before or during the run. The
// executor immediately takes a share of the remaining (not yet
// dispatched) work.
func (c *Coordinator) AddExecutor(e *Executor) error {
	c.mu.Lock()
	if !c.started {
		err := c.addExecutorLocked(e)
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	reply := make(chan interface{}, 1)
	select {
	case c.cmds <- cmd{kind: cmdAdd, exec: e, reply: reply}:
		err, _ := (<-reply).(error)
		return err
	case <-c.done:
		return ErrAlreadyStarted
	}
}

// RemoveExecutor gracefully unregisters an executor: its incomplete
// slots are reassigned to the survivors and resume from the last
// committed action, so no load is lost or duplicated.
func (c *Coordinator) RemoveExecutor(id string) {
	c.mu.Lock()
	if !c.started {
		c.removeExecutorLocked(id)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	select {
	case c.cmds <- cmd{kind: cmdRemove, id: id, reply: make(chan interface{}, 1)}:
	case <-c.done:
	}
}

// UpdateConfig mutates the live config. Only BatchSize, MaxInflight,
// MaxExecutors, HeartbeatInterval and HeartbeatTimeout may change;
// changes apply only to work not yet dispatched. Attempts to rewrite
// Seed, TotalActions, ShardCount or ShardStrategy are ignored.
func (c *Coordinator) UpdateConfig(fn func(*Config)) {
	c.mu.Lock()
	if !c.started {
		fn(&c.cfg)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	select {
	case c.cmds <- cmd{kind: cmdUpdateCfg, fn: fn, reply: make(chan interface{}, 1)}:
	case <-c.done:
	}
}

// Lag returns the number of dispatched-but-unacked actions per active
// executor, so slow or stuck executors are observable.
func (c *Coordinator) Lag() map[string]int {
	c.mu.Lock()
	if !c.started {
		out := make(map[string]int)
		for id, h := range c.executors {
			out[id] = h.inflight
		}
		c.mu.Unlock()
		return out
	}
	c.mu.Unlock()
	reply := make(chan interface{}, 1)
	select {
	case c.cmds <- cmd{kind: cmdLag, reply: reply}:
		m, _ := (<-reply).(map[string]int)
		return m
	case <-c.done:
		return nil
	}
}

// --- shared helpers (callable under c.mu or from the event loop) ---

func (c *Coordinator) addExecutorLocked(e *Executor) error {
	if e.Inbox == nil {
		e.Inbox = make(chan Batch, c.cfg.MaxInflight)
	}
	if _, ok := c.executors[e.ID]; ok {
		return ErrExecutorExists
	}
	active := 0
	for _, h := range c.executors {
		if h.state == ExecutorActive {
			active++
		}
	}
	if active >= c.cfg.MaxExecutors {
		return ErrTooManyExecutors
	}
	c.executors[e.ID] = &executorHandle{
		exec:     e,
		state:    ExecutorActive,
		slots:    make(map[int]bool),
		served:   make(map[int]bool),
		lastSeen: time.Now(),
	}
	c.order = append(c.order, e.ID)
	c.rebalanceLocked()
	if c.runCtx != nil {
		c.startExecutorLocked(e.ID)
	}
	return nil
}

func (c *Coordinator) removeExecutorLocked(id string) {
	h, ok := c.executors[id]
	if !ok || h.state != ExecutorActive {
		return
	}
	h.graceful = true
	h.state = ExecutorCompleted
	close(h.exec.Inbox)
	c.reassignLocked(h)
	c.rebalanceLocked()
}

// reassignLocked releases all of h's slots. Dispatch of each slot
// resumes at its committed prefix; unacked in-flight work is
// re-dispatched to whoever takes the slot over.
func (c *Coordinator) reassignLocked(h *executorHandle) {
	for s := range h.slots {
		st := c.slots[s]
		st.dispatched = st.committed
		st.assignedTo = ""
		delete(h.slots, s)
	}
}

// rebalanceLocked assigns unassigned slots and evens out the number of
// incomplete slots per active executor. Only the undispatched tail of a
// slot ever moves; committed progress stays put.
func (c *Coordinator) rebalanceLocked() {
	active := make([]string, 0, len(c.executors))
	for _, id := range c.order {
		if h := c.executors[id]; h.state == ExecutorActive {
			active = append(active, id)
		}
	}
	if len(active) == 0 {
		return
	}
	load := func(id string) int {
		n := 0
		for s := range c.executors[id].slots {
			if !c.slots[s].complete() {
				n++
			}
		}
		return n
	}
	assign := func(slot int, id string) {
		st := c.slots[slot]
		if st.assignedTo != "" && st.assignedTo != id {
			// The slot moves to another executor: resume from the
			// committed prefix. Anything still in flight on the
			// previous owner becomes a duplicate and is dropped by
			// the aggregator, so the slot can always make progress.
			st.dispatched = st.committed
			delete(c.executors[st.assignedTo].slots, slot)
		}
		st.assignedTo = id
		c.executors[id].slots[slot] = true
		c.executors[id].served[slot] = true
	}
	// 1. Assign unassigned, incomplete slots to the least loaded.
	for s, st := range c.slots {
		if st.complete() || st.assignedTo != "" {
			continue
		}
		best := active[0]
		for _, id := range active[1:] {
			if load(id) < load(best) {
				best = id
			}
		}
		assign(s, best)
	}
	// 2. Move slots from the busiest to the idlest while skew > 1.
	for {
		hi, lo := active[0], active[0]
		for _, id := range active[1:] {
			if load(id) > load(hi) {
				hi = id
			}
			if load(id) < load(lo) {
				lo = id
			}
		}
		if load(hi)-load(lo) <= 1 {
			return
		}
		// Move the incomplete slot with the most remaining work.
		moved := false
		bestSlot, bestRemain := -1, -1
		for s := range c.executors[hi].slots {
			st := c.slots[s]
			if st.complete() {
				continue
			}
			if rem := len(st.indices) - st.committed; rem > bestRemain {
				bestSlot, bestRemain = s, rem
				moved = true
			}
		}
		if !moved {
			return
		}
		assign(bestSlot, lo)
	}
}

func (c *Coordinator) startExecutorLocked(id string) {
	h := c.executors[id]
	go func() {
		h.exec.Run(c.runCtx, c.cfg.HeartbeatInterval, c.reports, c.heartbeats)
		c.exited <- id
	}()
}

// --- event handlers (event loop only) ---

func (c *Coordinator) onSample(s Sample) {
	if s.Slot < 0 || s.Slot >= len(c.slots) || s.ActionIndex < 0 || s.ActionIndex >= len(c.posOf) {
		return
	}
	st := c.slots[s.Slot]
	pos := c.posOf[s.ActionIndex]
	if pos >= len(st.acked) || st.indices[pos] != s.ActionIndex {
		return // inconsistent report; ignore
	}
	if h, ok := c.executors[s.ExecutorID]; ok && h.inflight > 0 {
		h.inflight--
	}
	if !st.acked[pos] {
		st.acked[pos] = true
		for st.committed < len(st.acked) && st.acked[st.committed] {
			st.committed++
		}
	}
	c.agg.Add(s)
}

func (c *Coordinator) onHeartbeat(hb Heartbeat) {
	if h, ok := c.executors[hb.ExecutorID]; ok && h.state == ExecutorActive {
		h.lastSeen = time.Now()
	}
}

// onExited handles an executor goroutine that returned. A graceful exit
// (removal, completion, cancel) is expected; anything else is a crash
// and its slots are taken over by the survivors.
func (c *Coordinator) onExited(id string) {
	h, ok := c.executors[id]
	if !ok {
		return
	}
	if h.graceful || h.state != ExecutorActive {
		return
	}
	h.state = ExecutorLost
	c.reassignLocked(h)
	c.rebalanceLocked()
}

func (c *Coordinator) onTick(now time.Time) {
	for _, id := range c.order {
		h := c.executors[id]
		if h.state != ExecutorActive {
			continue
		}
		if now.Sub(h.lastSeen) > c.cfg.HeartbeatTimeout {
			h.state = ExecutorTimeout
			h.graceful = true // its goroutine may still be stuck; don't wait for it
			c.reassignLocked(h)
		}
	}
	c.rebalanceLocked()
}

func (c *Coordinator) onCmd(cm cmd) {
	switch cm.kind {
	case cmdAdd:
		cm.reply <- c.addExecutorLocked(cm.exec)
	case cmdRemove:
		c.removeExecutorLocked(cm.id)
	case cmdUpdateCfg:
		immutable := c.cfg
		cm.fn(&c.cfg)
		// Immutable fields cannot be rewritten mid-run.
		c.cfg.Seed = immutable.Seed
		c.cfg.TotalActions = immutable.TotalActions
		c.cfg.ShardCount = immutable.ShardCount
		c.cfg.ShardStrategy = immutable.ShardStrategy
	case cmdLag:
		out := make(map[string]int)
		for id, h := range c.executors {
			if h.state == ExecutorActive {
				out[id] = h.inflight
			}
		}
		cm.reply <- out
	}
}

// dispatch pushes batches to executors that have spare in-flight
// capacity. Sends are non-blocking: a full inbox simply retries later,
// which is what bounds the backlog when an executor slows down.
func (c *Coordinator) dispatch() {
	for _, id := range c.order {
		h := c.executors[id]
		if h.state != ExecutorActive {
			continue
		}
		limit := c.cfg.MaxInflight * c.cfg.BatchSize
		slots := make([]int, 0, len(h.slots))
		for s := range h.slots {
			slots = append(slots, s)
		}
		sort.Ints(slots)
		for _, s := range slots {
			st := c.slots[s]
			for st.dispatched < len(st.indices) {
				n := c.cfg.BatchSize
				if rem := len(st.indices) - st.dispatched; rem < n {
					n = rem
				}
				if h.inflight+n > limit {
					break
				}
				actions := make([]Action, n)
				for i := 0; i < n; i++ {
					actions[i] = c.curve.Action(st.indices[st.dispatched+i])
				}
				sent := false
				select {
				case h.exec.Inbox <- Batch{Slot: s, Actions: actions}:
					st.dispatched += n
					h.inflight += n
					h.dispatched += n
					sent = true
				default:
					// Inbox full: backpressure. Retry on the next tick.
				}
				if !sent {
					break
				}
			}
		}
	}
}

func (c *Coordinator) allCommitted() bool {
	for _, st := range c.slots {
		if !st.complete() {
			return false
		}
	}
	return true
}

// finishExecutors closes every active executor's inbox and waits for
// the goroutines to wind down, so resources are released promptly.
func (c *Coordinator) finishExecutors(final ExecutorState) {
	waiting := 0
	for _, id := range c.order {
		h := c.executors[id]
		if h.state != ExecutorActive {
			continue
		}
		h.graceful = true
		h.state = final
		close(h.exec.Inbox)
		waiting++
	}
	deadline := time.After(2 * time.Second)
	for waiting > 0 {
		select {
		case <-c.exited:
			waiting--
		case cm := <-c.cmds:
			if cm.reply != nil {
				cm.reply <- ErrAlreadyStarted
			}
		case <-deadline:
			return
		}
	}
}

func (c *Coordinator) buildReport(canceled bool) *Report {
	completed, dups, pcts, throughput, timeline := c.agg.summary(time.Since(c.startTime))
	perExec := c.agg.SamplesPerExecutor()
	rep := &Report{
		TotalActions: c.cfg.TotalActions,
		Completed:    completed,
		Duplicates:   dups,
		Throughput:   throughput,
		Percentiles:  pcts,
		Timeline:     timeline,
		Canceled:     canceled,
	}
	for _, id := range c.order {
		h := c.executors[id]
		slots := make([]int, 0, len(h.served))
		for s := range h.served {
			slots = append(slots, s)
		}
		sort.Ints(slots)
		rep.Executors = append(rep.Executors, ExecutorReport{
			ID:         id,
			State:      h.state,
			Slots:      slots,
			Samples:    perExec[id],
			Dispatched: h.dispatched,
			Committed:  perExec[id],
		})
	}
	return rep
}

// Run executes the whole run and returns the aggregated report. It
// returns when every action is committed, or early with a partial
// report (Canceled=true) when ctx is canceled.
func (c *Coordinator) Run(ctx context.Context) (*Report, error) {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil, ErrAlreadyStarted
	}
	c.started = true
	c.runCtx = ctx
	c.startTime = time.Now()
	for _, id := range c.order {
		if h := c.executors[id]; h.state == ExecutorActive {
			c.startExecutorLocked(id)
		}
	}
	c.mu.Unlock()
	defer close(c.done)

	tick := time.NewTicker(c.cfg.HeartbeatInterval / 2)
	defer tick.Stop()
	for {
		if c.allCommitted() {
			c.finishExecutors(ExecutorCompleted)
			return c.buildReport(false), nil
		}
		select {
		case <-ctx.Done():
			c.finishExecutors(ExecutorCanceled)
			return c.buildReport(true), ctx.Err()
		case s := <-c.reports:
			c.onSample(s)
		case hb := <-c.heartbeats:
			c.onHeartbeat(hb)
		case id := <-c.exited:
			c.onExited(id)
		case cm := <-c.cmds:
			c.onCmd(cm)
		case <-tick.C:
			c.onTick(time.Now())
		}
		c.dispatch()
	}
}
