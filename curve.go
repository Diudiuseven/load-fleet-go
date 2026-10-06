package fleet

import "time"

// defaultActionInterval is used when Config.ActionInterval is zero.
const defaultActionInterval = time.Millisecond

// Curve is a deterministic load curve: the same seed and config always
// produce the same action sequence, regardless of executor count.
type Curve struct {
	actions []Action
}

// NewCurve builds the deterministic action sequence for cfg. Every
// action's payload is a pure function of (seed, index); its scheduled
// offset is a pure function of index. Rebuilding the curve with the same
// config yields the identical sequence.
func NewCurve(cfg Config) *Curve {
	interval := cfg.ActionInterval
	if interval <= 0 {
		interval = defaultActionInterval
	}
	n := cfg.TotalActions
	actions := make([]Action, n)
	for i := 0; i < n; i++ {
		actions[i] = Action{
			Index:   i,
			Slot:    slotOf(cfg.ShardStrategy, cfg.Seed, cfg.ShardCount, i, n),
			At:      time.Duration(i) * interval,
			Payload: mix64(uint64(cfg.Seed)*0x9e3779b97f4a7c15 ^ uint64(i)),
		}
	}
	return &Curve{actions: actions}
}

// Len returns the total number of actions.
func (c *Curve) Len() int { return len(c.actions) }

// Action returns the action at global index i.
func (c *Curve) Action(i int) Action { return c.actions[i] }
