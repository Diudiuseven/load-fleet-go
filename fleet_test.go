package fleet

import (
	"context"
	"encoding/json"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func testCfg() Config {
	return Config{
		Seed:              42,
		TotalActions:      2000,
		ShardCount:        16,
		ShardStrategy:     ShardModulo,
		MaxExecutors:      8,
		BatchSize:         10,
		MaxInflight:       4,
		HeartbeatInterval: 5 * time.Millisecond,
		HeartbeatTimeout:  100 * time.Millisecond,
	}
}

func eventually(t *testing.T, d time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", d, msg)
}

// runFleet runs a coordinator with n plain executors to completion.
func runFleet(t *testing.T, cfg Config, ids ...string) *Report {
	t.Helper()
	c := NewCoordinator(cfg)
	for _, id := range ids {
		if err := c.AddExecutor(NewExecutor(id, cfg.MaxInflight)); err != nil {
			t.Fatalf("add executor %s: %v", id, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rep, err := c.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return rep
}

func TestShardingCompleteDisjoint(t *testing.T) {
	for _, strat := range []ShardStrategy{ShardModulo, ShardHash, ShardRange} {
		cfg := testCfg()
		cfg.ShardStrategy = strat
		cfg.TotalActions = 1000
		cfg.ShardCount = 7
		buckets := bucketIndices(strat, cfg.Seed, cfg.ShardCount, cfg.TotalActions)
		seen := make(map[int]int)
		total := 0
		for s, idxs := range buckets {
			prev := -1
			for _, i := range idxs {
				if i < 0 || i >= cfg.TotalActions {
					t.Fatalf("strategy %d slot %d: index %d out of range", strat, s, i)
				}
				if i <= prev {
					t.Fatalf("strategy %d slot %d: indices not ascending", strat, s)
				}
				prev = i
				if _, dup := seen[i]; dup {
					t.Fatalf("strategy %d: index %d in two slots", strat, i)
				}
				seen[i] = s
				total++
			}
		}
		if total != cfg.TotalActions {
			t.Fatalf("strategy %d: covered %d of %d actions", strat, total, cfg.TotalActions)
		}
	}
}

func TestDeterministicCurve(t *testing.T) {
	cfg := testCfg()
	a, b := NewCurve(cfg), NewCurve(cfg)
	if a.Len() != b.Len() || a.Len() != cfg.TotalActions {
		t.Fatalf("curve length mismatch")
	}
	for i := 0; i < a.Len(); i++ {
		if a.Action(i) != b.Action(i) {
			t.Fatalf("action %d differs between rebuilds", i)
		}
		if a.Action(i).Slot != slotOf(cfg.ShardStrategy, cfg.Seed, cfg.ShardCount, i, cfg.TotalActions) {
			t.Fatalf("action %d slot inconsistent with slotOf", i)
		}
	}
}

func TestSingleVsMultiEquivalence(t *testing.T) {
	cfg := testCfg()
	single := runFleet(t, cfg, "e1")
	multi := runFleet(t, cfg, "e1", "e2", "e3", "e4")
	if single.Completed != cfg.TotalActions || multi.Completed != cfg.TotalActions {
		t.Fatalf("completed: single=%d multi=%d want %d", single.Completed, multi.Completed, cfg.TotalActions)
	}
	for k, v := range single.Percentiles {
		if multi.Percentiles[k] != v {
			t.Fatalf("percentile %s: single=%v multi=%v", k, v, multi.Percentiles[k])
		}
	}
	if len(single.Timeline) != len(multi.Timeline) {
		t.Fatalf("timeline length: single=%d multi=%d", len(single.Timeline), len(multi.Timeline))
	}
	for i := range single.Timeline {
		if single.Timeline[i] != multi.Timeline[i] {
			t.Fatalf("timeline bucket %d: single=%+v multi=%+v", i, single.Timeline[i], multi.Timeline[i])
		}
	}
	// Per-executor sample counts must sum to the total exactly once.
	sum := 0
	slots := make(map[int]bool)
	for _, er := range multi.Executors {
		sum += er.Samples
		for _, s := range er.Slots {
			slots[s] = true
		}
	}
	if sum != cfg.TotalActions {
		t.Fatalf("per-executor samples sum=%d want %d", sum, cfg.TotalActions)
	}
	if len(slots) != cfg.ShardCount {
		t.Fatalf("served slots cover %d of %d", len(slots), cfg.ShardCount)
	}
}

func TestDeterministicAcrossExecutorCounts(t *testing.T) {
	cfg := testCfg()
	r1 := runFleet(t, cfg, "e1")
	r2 := runFleet(t, cfg, "e1", "e2")
	r3 := runFleet(t, cfg, "e1", "e2", "e3", "e4", "e5")
	for k, v := range r1.Percentiles {
		if r2.Percentiles[k] != v || r3.Percentiles[k] != v {
			t.Fatalf("percentile %s diverges across executor counts: %v/%v/%v",
				k, v, r2.Percentiles[k], r3.Percentiles[k])
		}
	}
	if r1.Completed != r2.Completed || r2.Completed != r3.Completed {
		t.Fatalf("completed diverges: %d/%d/%d", r1.Completed, r2.Completed, r3.Completed)
	}
}

func TestDynamicScaleUpDown(t *testing.T) {
	cfg := testCfg()
	c := NewCoordinator(cfg)
	if err := c.AddExecutor(NewExecutor("e1", cfg.MaxInflight)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	type result struct {
		rep *Report
		err error
	}
	done := make(chan result, 1)
	go func() {
		rep, err := c.Run(ctx)
		done <- result{rep, err}
	}()

	// Scale up while running.
	eventually(t, 5*time.Second, "some progress before scale-up", func() bool {
		return c.Aggregator().Completed() > 100
	})
	if err := c.AddExecutor(NewExecutor("e2", cfg.MaxInflight)); err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor(NewExecutor("e3", cfg.MaxInflight)); err != nil {
		t.Fatal(err)
	}
	// Scale down while running.
	eventually(t, 5*time.Second, "more progress before scale-down", func() bool {
		return c.Aggregator().Completed() > 500
	})
	c.RemoveExecutor("e1")

	var r result
	select {
	case r = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("run did not finish")
	}
	if r.err != nil {
		t.Fatalf("run: %v", r.err)
	}
	if r.rep.Completed != cfg.TotalActions {
		t.Fatalf("completed=%d want %d (no loss, no dup)", r.rep.Completed, cfg.TotalActions)
	}
	// Timeline must align to one axis and cover every action once.
	sum := 0
	for _, b := range r.rep.Timeline {
		sum += b.Count
	}
	if sum != cfg.TotalActions {
		t.Fatalf("timeline covers %d of %d actions", sum, cfg.TotalActions)
	}
	// All three executors must appear in the report.
	if len(r.rep.Executors) != 3 {
		t.Fatalf("executors in report=%d want 3", len(r.rep.Executors))
	}
}

func TestDuplicateReportsDropped(t *testing.T) {
	a := NewAggregator(time.Millisecond)
	s := Sample{ActionIndex: 7, Slot: 3, ExecutorID: "e1", Latency: time.Millisecond}
	if a.Add(s) {
		t.Fatal("first add reported as duplicate")
	}
	if !a.Add(s) {
		t.Fatal("resend not detected as duplicate")
	}
	s.ExecutorID = "e2" // same action from a takeover executor
	if !a.Add(s) {
		t.Fatal("takeover resend not detected as duplicate")
	}
	if a.Completed() != 1 || a.Duplicates() != 2 {
		t.Fatalf("completed=%d dups=%d want 1/2", a.Completed(), a.Duplicates())
	}
}

func TestFailureTakeover(t *testing.T) {
	cfg := testCfg()
	c := NewCoordinator(cfg)
	bad := NewExecutor("bad", cfg.MaxInflight)
	bad.FailAfter = 2 // crashes after 2 batches
	if err := c.AddExecutor(bad); err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor(NewExecutor("good", cfg.MaxInflight)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rep, err := c.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.Completed != cfg.TotalActions {
		t.Fatalf("completed=%d want %d: failed slots were not taken over", rep.Completed, cfg.TotalActions)
	}
	states := map[string]ExecutorState{}
	for _, er := range rep.Executors {
		states[er.ID] = er.State
	}
	if states["bad"] != ExecutorLost {
		t.Fatalf("bad executor state=%s want lost", states["bad"])
	}
	if states["good"] != ExecutorCompleted {
		t.Fatalf("good executor state=%s want completed", states["good"])
	}
}

func TestTimeoutTakeover(t *testing.T) {
	cfg := testCfg()
	c := NewCoordinator(cfg)
	mute := &atomic.Bool{}
	mute.Store(true)
	deaf := NewExecutor("deaf", cfg.MaxInflight)
	deaf.Mute = mute
	if err := c.AddExecutor(deaf); err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor(NewExecutor("alive", cfg.MaxInflight)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rep, err := c.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.Completed != cfg.TotalActions {
		t.Fatalf("completed=%d want %d", rep.Completed, cfg.TotalActions)
	}
	states := map[string]ExecutorState{}
	for _, er := range rep.Executors {
		states[er.ID] = er.State
	}
	if states["deaf"] != ExecutorTimeout {
		t.Fatalf("deaf executor state=%s want timeout", states["deaf"])
	}
}

func TestCancelConverges(t *testing.T) {
	cfg := testCfg()
	cfg.TotalActions = 20000
	c := NewCoordinator(cfg)
	slow := NewExecutor("slow", cfg.MaxInflight)
	slow.WorkDelay = 2 * time.Millisecond
	if err := c.AddExecutor(slow); err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *Report, 1)
	go func() {
		rep, _ := c.Run(ctx)
		done <- rep
	}()
	eventually(t, 5*time.Second, "some progress", func() bool {
		return c.Aggregator().Completed() > 50
	})
	cancel()
	select {
	case rep := <-done:
		if !rep.Canceled {
			t.Fatal("report not marked canceled")
		}
		if rep.Completed >= cfg.TotalActions {
			t.Fatalf("completed=%d: cancel did not stop the run early", rep.Completed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not converge after cancel")
	}
	// Executor goroutines must be released promptly.
	eventually(t, 3*time.Second, "goroutines released", func() bool {
		return runtime.NumGoroutine() <= before+1
	})
}

func TestBackpressureBoundsLag(t *testing.T) {
	cfg := testCfg()
	c := NewCoordinator(cfg)
	stall := &atomic.Bool{}
	stall.Store(true)
	stuck := NewExecutor("stuck", cfg.MaxInflight)
	stuck.Stall = stall
	if err := c.AddExecutor(stuck); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan *Report, 1)
	go func() {
		rep, _ := c.Run(ctx)
		done <- rep
	}()
	limit := cfg.MaxInflight * cfg.BatchSize
	eventually(t, 5*time.Second, "lag rises to the backpressure limit", func() bool {
		return c.Lag()["stuck"] > 0
	})
	// While stalled, the backlog must never exceed the configured bound.
	for i := 0; i < 50; i++ {
		if lag := c.Lag()["stuck"]; lag > limit {
			t.Fatalf("lag=%d exceeds backpressure limit %d", lag, limit)
		}
		time.Sleep(2 * time.Millisecond)
	}
	// Unstick: the run must drain and complete.
	stall.Store(false)
	select {
	case rep := <-done:
		if rep.Completed != cfg.TotalActions {
			t.Fatalf("completed=%d want %d", rep.Completed, cfg.TotalActions)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not finish after unstalling")
	}
}

func TestUpdateConfigAffectsOnlyFuture(t *testing.T) {
	cfg := testCfg()
	c := NewCoordinator(cfg)
	if err := c.AddExecutor(NewExecutor("e1", cfg.MaxInflight)); err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor(NewExecutor("e2", cfg.MaxInflight)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan *Report, 1)
	go func() {
		rep, _ := c.Run(ctx)
		done <- rep
	}()
	eventually(t, 5*time.Second, "some progress", func() bool {
		return c.Aggregator().Completed() > 50
	})
	c.UpdateConfig(func(live *Config) {
		live.BatchSize = 4
		live.MaxInflight = 2
		live.Seed = 999       // must be ignored
		live.TotalActions = 5 // must be ignored
		live.ShardCount = 3   // must be ignored
	})
	rep := <-done
	if rep.Completed != cfg.TotalActions {
		t.Fatalf("completed=%d want %d: config change rewrote history", rep.Completed, cfg.TotalActions)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.BatchSize != 4 || c.cfg.MaxInflight != 2 {
		t.Fatalf("mutable config not applied: %+v", c.cfg)
	}
	if c.cfg.Seed != 42 || c.cfg.TotalActions != 2000 || c.cfg.ShardCount != 16 {
		t.Fatalf("immutable config rewritten: %+v", c.cfg)
	}
}

func TestMaxExecutorsEnforced(t *testing.T) {
	cfg := testCfg()
	cfg.MaxExecutors = 2
	c := NewCoordinator(cfg)
	if err := c.AddExecutor(NewExecutor("e1", 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor(NewExecutor("e2", 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.AddExecutor(NewExecutor("e3", 1)); err != ErrTooManyExecutors {
		t.Fatalf("err=%v want ErrTooManyExecutors", err)
	}
	if err := c.AddExecutor(NewExecutor("e2", 1)); err != ErrExecutorExists {
		t.Fatalf("err=%v want ErrExecutorExists", err)
	}
}

func TestReportMachineReadable(t *testing.T) {
	cfg := testCfg()
	rep := runFleet(t, cfg, "e1", "e2")
	data, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Report
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Completed != rep.Completed || back.TotalActions != rep.TotalActions {
		t.Fatalf("round-trip mismatch: %+v", back)
	}
	if len(back.Executors) != 2 || back.Executors[0].ID == "" {
		t.Fatalf("executor reports missing: %+v", back.Executors)
	}
	if len(back.Percentiles) == 0 {
		t.Fatal("percentiles missing")
	}
}
