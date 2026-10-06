package fleet

import (
	"context"
	"sync/atomic"
	"time"
)

// Batch is a unit of dispatched work for one slot.
type Batch struct {
	Slot    int
	Actions []Action
}

// Heartbeat is a liveness signal sent by an executor to the coordinator.
type Heartbeat struct {
	ExecutorID string
	At         time.Time
	Processed  int // total samples reported so far
}

// Executor simulates a load executor inside the local process. It
// receives batches over an in-memory channel and reports samples back
// over another in-memory channel. No external service is involved.
type Executor struct {
	ID    string
	Inbox chan Batch

	// WorkDelay simulates a slow executor: it is slept once per batch.
	WorkDelay time.Duration
	// Stall, when non-nil and true, makes the executor stop consuming
	// its inbox while still sending heartbeats (a "stuck" executor).
	Stall *atomic.Bool
	// Mute, when non-nil and true, makes the executor stop consuming
	// and stop heartbeating (simulates a partitioned executor that is
	// still alive but unreachable). The coordinator will time it out.
	Mute *atomic.Bool
	// FailAfter, when > 0, makes the executor die silently after
	// processing that many batches (simulates a crash).
	FailAfter int
}

// NewExecutor creates an executor with the given id and a bounded inbox
// so that dispatch is subject to backpressure.
func NewExecutor(id string, inboxCap int) *Executor {
	if inboxCap < 1 {
		inboxCap = 1
	}
	return &Executor{ID: id, Inbox: make(chan Batch, inboxCap)}
}

// latencyOf derives the deterministic simulated latency of an action.
// It is a pure function of the action payload, so the same action always
// yields the same latency no matter which executor runs it.
func latencyOf(a Action) time.Duration {
	return time.Duration(a.Payload%1000) * time.Microsecond
}

// Run consumes batches until the context is canceled, the inbox is
// closed, or the executor fails. It always returns, releasing its
// goroutine; cancellation makes it exit promptly.
func (e *Executor) Run(ctx context.Context, heartbeatInterval time.Duration, reports chan<- Sample, heartbeats chan<- Heartbeat) {
	if heartbeatInterval <= 0 {
		heartbeatInterval = 50 * time.Millisecond
	}
	hb := time.NewTicker(heartbeatInterval)
	defer hb.Stop()
	processed := 0
	batches := 0
	for {
		if e.Mute != nil && e.Mute.Load() {
			// Unreachable: neither consume nor heartbeat.
			select {
			case <-ctx.Done():
				return
			case <-time.After(heartbeatInterval):
			}
			continue
		}
		if e.Stall != nil && e.Stall.Load() {
			// Stuck: keep heartbeating but stop consuming.
			select {
			case <-ctx.Done():
				return
			case heartbeats <- Heartbeat{ExecutorID: e.ID, At: time.Now(), Processed: processed}:
			case <-time.After(heartbeatInterval):
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case b, ok := <-e.Inbox:
			if !ok {
				return
			}
			batches++
			if e.FailAfter > 0 && batches > e.FailAfter {
				return // silent crash: no more heartbeats
			}
			if e.WorkDelay > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(e.WorkDelay):
				}
			}
			for _, a := range b.Actions {
				s := Sample{
					ActionIndex: a.Index,
					Slot:        a.Slot,
					ExecutorID:  e.ID,
					At:          a.At,
					Latency:     latencyOf(a),
				}
				select {
				case <-ctx.Done():
					return
				case reports <- s:
					processed++
				}
			}
		case heartbeats <- Heartbeat{ExecutorID: e.ID, At: time.Now(), Processed: processed}:
		}
	}
}
