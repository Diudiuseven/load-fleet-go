package fleet

import (
	"context"
	"time"
)

// Run drives the run until all actions are committed, the context is
// cancelled, or no live executors remain. It returns the final,
// machine-readable report in every case.
func (c *Coordinator) Run(ctx context.Context) (*Report, error) {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil, errAlreadyStarted
	}
	c.started = true
	// Start executor goroutines under a child context so cancellation
	// converges every executor promptly.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.runCtx = runCtx
	hbTimeout := c.cfg.HeartbeatTimeout
	for _, id := range c.order {
		st := c.executors[id]
		if st.status == StatusActive && st.cancel == nil {
			ectx, ecancel := context.WithCancel(runCtx)
			st.cancel = ecancel
			go st.ex.run(ectx)
		}
	}
	c.mu.Unlock()

	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	hbCheck := time.NewTicker(hbTimeout / 2)
	defer hbCheck.Stop()

	for {
		select {
		case <-ctx.Done():
			c.mu.Lock()
			rep := c.buildReportLocked(true)
			c.mu.Unlock()
			return rep, ctx.Err()
		case rb := <-c.reportCh:
			c.mu.Lock()
			c.handleReportLocked(rb)
			c.dispatchLocked()
			done := c.committedLocked() == c.cfg.TotalActions
			c.mu.Unlock()
			if done {
				cancel()
				c.mu.Lock()
				rep := c.buildReportLocked(false)
				c.mu.Unlock()
				return rep, nil
			}
		case <-tick.C:
			c.mu.Lock()
			c.dispatchLocked()
			done := c.committedLocked() == c.cfg.TotalActions
			noLive := c.liveLocked() == 0
			c.mu.Unlock()
			if done || noLive {
				cancel()
				c.mu.Lock()
				rep := c.buildReportLocked(false)
				c.mu.Unlock()
				return rep, nil
			}
		case <-hbCheck.C:
			c.mu.Lock()
			c.checkHeartbeatsLocked()
			c.mu.Unlock()
		}
	}
}

var errAlreadyStarted = errorString("fleet: coordinator already started")

type errorString string

func (e errorString) Error() string { return string(e) }

// handleReportLocked merges one executor report: dedup samples by
// action index, advance committed state, release in-flight slots.
func (c *Coordinator) handleReportLocked(rb ReportBatch) {
	st, ok := c.executors[rb.ExecutorID]
	if !ok {
		return
	}
	st.lastSeen = c.now()
	if len(rb.Samples) > 0 && st.inFlight > 0 {
		st.inFlight--
	}
	for _, s := range rb.Samples {
		idx := s.ActionIndex
		if idx < 0 || idx >= c.cfg.TotalActions {
			continue
		}
		delete(st.inFlightIdx, idx)
		if c.agg.add(s, c.actions[idx].ScheduledAt) {
			if c.hook != nil {
				c.hook("commit", idx, rb.ExecutorID)
			}
			c.state[idx] = idxCommitted
			st.samples++
		} else if c.hook != nil {
			c.hook("dup", idx, rb.ExecutorID)
		}
	}
}

// dispatchLocked fills spare in-flight capacity of every live executor
// with batches popped from its owned shard queues.
func (c *Coordinator) dispatchLocked() {
	for _, id := range c.order {
		st := c.executors[id]
		if st.status != StatusActive {
			continue
		}
		limit := c.cfg.MaxInFlight
		if st.inboxCap < limit {
			limit = st.inboxCap // inbox capacity is fixed at join time
		}
		for st.inFlight < limit {
			s := c.nextShardForLocked(id)
			if s < 0 {
				break
			}
			b := Batch{Shard: s, Actions: make([]Action, 0, c.cfg.BatchSize)}
			for len(b.Actions) < c.cfg.BatchSize && len(c.queues[s]) > 0 {
				idx := c.queues[s][0]
				c.queues[s] = c.queues[s][1:]
				// A late report may have committed this index while it
				// sat in the queue; never redispatch committed work.
				if c.state[idx] != idxPending {
					continue
				}
				if c.hook != nil {
					c.hook("dispatch", idx, id)
				}
				c.state[idx] = idxDispatched
				c.shardOf[idx] = s
				c.actions[idx] = buildAction(c.cfg, idx)
				b.Actions = append(b.Actions, c.actions[idx])
				st.inFlightIdx[idx] = true
			}
			if len(b.Actions) == 0 {
				continue // queue held only stale entries; try next shard
			}
			st.inFlight++
			st.ex.inbox <- b // never blocks: inFlight < limit <= cap(inbox)
		}
	}
}

// nextShardForLocked picks a non-empty shard queue owned by id.
func (c *Coordinator) nextShardForLocked(id string) int {
	// find executor position
	pos := -1
	for i, x := range c.order {
		if x == id {
			pos = i
			break
		}
	}
	if pos < 0 {
		return -1
	}
	for s := range c.queues {
		if c.owner[s] == pos && len(c.queues[s]) > 0 {
			return s
		}
	}
	return -1
}

// checkHeartbeatsLocked marks silent executors lost and reassigns their
// work so no gap is left behind.
func (c *Coordinator) checkHeartbeatsLocked() {
	deadline := c.now().Add(-c.cfg.HeartbeatTimeout)
	for _, id := range append([]string(nil), c.order...) {
		st := c.executors[id]
		if st.status == StatusActive && st.lastSeen.Before(deadline) {
			c.removeLocked(id, StatusLost)
		}
	}
}

func (c *Coordinator) committedLocked() int {
	n := 0
	for _, s := range c.state {
		if s == idxCommitted {
			n++
		}
	}
	return n
}
