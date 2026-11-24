package network

import "context"

// Run is a network's run bookkeeping: which run is in force (Gen counts
// them, so events and results of an older one are dropped) and its
// context, ended when the run is retired. The network's own lock guards it;
// a network's retire calls Retire and then ends what else the run held (its
// client, its sockets, its timers).
type Run struct {
	Gen    int
	ctx    context.Context
	cancel context.CancelFunc
}

// Begin starts a run under parent: its context, and its number (the Gen in
// force).
func (r *Run) Begin(parent context.Context) (ctx context.Context, gen int) {
	ctx, r.cancel = context.WithCancel(parent)
	r.ctx = ctx
	return ctx, r.Gen
}

// Retire ends the run in force, if any: its context, and (by moving Gen
// on) whatever is still to come from it.
func (r *Run) Retire() {
	r.Gen++
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.ctx = nil
}

// Ctx is the run's context, nil when no run is in force.
func (r *Run) Ctx() context.Context { return r.ctx }
