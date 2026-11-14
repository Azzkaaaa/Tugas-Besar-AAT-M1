// Package leader elects the replica that runs the aggregator's single-instance work (pollers,
// outbox pruner, and the outbox publisher on single-consumer stores). It imports no database
// driver: lease-based electors run on any LeaseStore, which storage backends implement.
//
// Election is best effort by design. A lease elector holds leadership until the lease expires and
// stops its leader at a local deadline before that (self-fencing), so a new leader normally starts
// only after the old one stopped. A process pause longer than the margin can still let the two
// overlap briefly; event IDs are deterministic per hazard revision, so work repeated during an
// overlap publishes the same event_id, which consumers deduplicate.
package leader

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// Elector runs fn while this process leads. fn's context is cancelled when leadership ends, and
// fn must return soon after. Run restarts fn on re-election and returns once ctx is done.
type Elector interface {
	Run(ctx context.Context, fn func(ctx context.Context))
}

// LeaseStore is a named lease with one holder that expires after ttl unless renewed. Expiry is
// measured on the store's clock.
type LeaseStore interface {
	// Acquire takes the lease when it is free or expired (or already held by holder).
	Acquire(ctx context.Context, name, holder string, ttl time.Duration) (bool, error)
	// Renew extends the lease when holder still holds it; false means it was lost.
	Renew(ctx context.Context, name, holder string, ttl time.Duration) (bool, error)
	// Release gives the lease up when holder holds it.
	Release(ctx context.Context, name, holder string) error
}

// Single is the elector of a deliberately single-replica deployment: always leader.
type Single struct{}

func (Single) Run(ctx context.Context, fn func(ctx context.Context)) {
	fn(ctx)
	<-ctx.Done()
}

// Identity returns the holder name of this process: LEADER_IDENTITY, else POD_NAME, else
// hostname plus pid and a random suffix (unique per process start).
func Identity() string {
	if v := os.Getenv("LEADER_IDENTITY"); v != "" {
		return v
	}
	if v := os.Getenv("POD_NAME"); v != "" {
		return v
	}
	host, _ := os.Hostname()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
}
