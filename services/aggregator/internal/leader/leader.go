// Package leader elects the replica that runs the aggregator's single-instance work (pollers,
// outbox pruner, and the outbox publisher on single-consumer stores). It imports no database
// driver: lease-based electors run on any LeaseStore, which storage backends implement.
//
// Each acquisition has a unique holder and, for FencingStore implementations,
// a storage-issued authority context. Those backends enforce authority at their
// mutation boundary; local cancellation and deterministic IDs are not fencing.
// Electors without FencingStore retain timer-based best-effort exclusion.
package leader

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
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

// Electors gives each leader-only job its own elector, so jobs fail over independently and can
// lead on different replicas.
type Electors interface {
	For(job string) Elector
}

// Pool counts the leases held by this process; electors of one process share it to spread jobs
// across replicas. A nil Pool counts nothing.
type Pool struct{ held atomic.Int32 }

// Held returns how many leases the process holds.
func (p *Pool) Held() int {
	if p == nil {
		return 0
	}
	return int(p.held.Load())
}

// Add records n more (or, negative, fewer) leases held. Electors call it.
func (p *Pool) Add(n int32) {
	if p != nil {
		p.held.Add(n)
	}
}

// SingleElectors elects this process for every job.
type SingleElectors struct{}

func (SingleElectors) For(string) Elector { return Single{} }

// LeaseElectors gives each job a Lease named Prefix + "-" + job, all on one store and Pool.
type LeaseElectors struct {
	Store      LeaseStore
	Prefix     string
	Holder     string
	TTL        time.Duration
	RenewEvery time.Duration
	RetryEvery time.Duration
	Margin     time.Duration
	Pool       *Pool
	Yield      *YieldConfig
}

// For returns the lease elector of job.
func (e *LeaseElectors) For(job string) Elector { return e.lease(job) }

func (e *LeaseElectors) lease(job string) *Lease {
	return &Lease{
		Store: e.Store, Name: e.Prefix + "-" + job, Holder: e.Holder,
		TTL: e.TTL, RenewEvery: e.RenewEvery, RetryEvery: e.RetryEvery, Margin: e.Margin, Pool: e.Pool,
		Yield: e.Yield,
	}
}

// Validate checks the shared settings.
func (e *LeaseElectors) Validate() error {
	if e.Prefix == "" {
		return errors.New("lease electors need a lease name prefix")
	}
	return e.lease("validate").Validate()
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

func newTermIdentity() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("leader term entropy unavailable")
	}
	return hex.EncodeToString(b)
}
