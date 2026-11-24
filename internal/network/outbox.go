package network

import (
	"context"
	"sync"
)

// Outbox is a network's line of sends: one goroutine sends them, in the
// order they were put in line. Set its funcs before the first Enqueue.
type Outbox struct {
	// Begin is asked as a goroutine starts sending: the context the sends go
	// under and true when the network is running, having counted the
	// goroutine in the network's work (End uncounts it); false when it is
	// not, and then each send in line is Stopped.
	Begin func() (ctx context.Context, ok bool)
	End   func()
	// Deliver sends one send.
	Deliver func(ctx context.Context, id string)
	// Stopped fails a send whose turn came after the network began stopping
	// (or before it started): it never went.
	Stopped func(id string)
	// Busy, when set, counts each send from Enqueue until its turn has
	// ended.
	Busy func(delta int)

	mu       sync.Mutex
	queue    []string
	draining bool
}

// Enqueue puts a send in line, and starts the goroutine that sends them if
// none is.
func (o *Outbox) Enqueue(id string) {
	if o.Busy != nil {
		o.Busy(1)
	}
	o.mu.Lock()
	o.queue = append(o.queue, id)
	if o.draining {
		o.mu.Unlock()
		return
	}
	o.draining = true
	o.mu.Unlock()
	ctx, ok := o.Begin()
	if !ok {
		o.drain(nil)
		return
	}
	go func() {
		defer o.End()
		o.drain(ctx)
	}()
}

// drain sends what is in line until none is left. A send whose turn comes
// after the network began stopping never went: Stopped.
func (o *Outbox) drain(ctx context.Context) {
	for {
		o.mu.Lock()
		if len(o.queue) == 0 {
			o.draining = false
			o.mu.Unlock()
			return
		}
		id := o.queue[0]
		o.queue = o.queue[1:]
		o.mu.Unlock()
		if ctx == nil || ctx.Err() != nil {
			o.Stopped(id)
		} else {
			o.Deliver(ctx, id)
		}
		if o.Busy != nil {
			o.Busy(-1)
		}
	}
}
