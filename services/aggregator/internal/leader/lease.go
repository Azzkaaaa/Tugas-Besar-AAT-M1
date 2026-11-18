package leader

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"
)

// Lease elects through a LeaseStore.
//
// An elapsed-time deadline stops ordinary terms promptly. It does not authorize
// writes: a suspended goroutine may resume before cancellation is delivered.
// FencingStore activation binds storage authority to the job context before it
// starts, and storage must validate that authority atomically with mutations.
// Margin also leaves room for bounded requests and orderly handoff.
type Lease struct {
	Store      LeaseStore
	Name       string
	Holder     string
	TTL        time.Duration
	RenewEvery time.Duration
	RetryEvery time.Duration
	Margin     time.Duration
	// Pool, when set, is shared by all electors of the process: it counts the leases held, and a
	// process holding more waits longer before asking for another (see Run).
	Pool *Pool
	// Yield, when set, lets the job yield or release its lease (see MaybeYield and Release).
	Yield *YieldConfig
	// Logf logs leadership changes (log.Printf when nil).
	Logf func(format string, args ...any)

	handoffOnce sync.Once
	handoff     *Handoff
}

func (l *Lease) state() *Handoff {
	l.handoffOnce.Do(func() { l.handoff = NewHandoff(l.Yield) })
	return l.handoff
}

// Defaults for Lease timing: a new leader can take over 15s after the last renewal the store
// applied, while the old one stopped itself at most 10s after its last successful one.
const (
	DefaultTTL        = 15 * time.Second
	DefaultRenewEvery = 5 * time.Second
	DefaultRetryEvery = 5 * time.Second
	DefaultMargin     = 5 * time.Second
)

// Validate checks the timing: renewals must fit before the deadline.
func (l *Lease) Validate() error {
	switch {
	case l.Store == nil:
		return errors.New("lease elector needs a lease store")
	case l.Name == "" || l.Holder == "":
		return errors.New("lease elector needs a lease name and a holder identity")
	case l.TTL <= 0 || l.RenewEvery <= 0 || l.RetryEvery <= 0 || l.Margin <= 0:
		return errors.New("lease TTL, renew interval, retry interval and margin must be positive")
	case l.RenewEvery >= l.TTL-l.Margin:
		return fmt.Errorf("lease renew interval %s must be shorter than TTL - margin (%s - %s)", l.RenewEvery, l.TTL, l.Margin)
	}
	if l.Yield != nil {
		return l.Yield.Validate()
	}
	return nil
}

func (l *Lease) logf(format string, args ...any) {
	if l.Logf != nil {
		l.Logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// Run campaigns for the lease until ctx is done. Attempts are spread out so that jobs under
// separate leases tend to land on different replicas: each attempt waits a random jitter below
// RetryEvery (plus RetryEvery after a refusal), and then RetryEvery more for every lease this
// process already holds, checked just before asking, so a less-loaded replica asks first. A lone
// replica still gets every lease, within about RetryEvery x the number of leases.
func (l *Lease) Run(ctx context.Context, fn func(ctx context.Context)) {
	wait := jitter(l.RetryEvery)
	for ctx.Err() == nil {
		if !sleep(ctx, wait) {
			return
		}
		if held := l.Pool.Held(); held > 0 && !sleep(ctx, l.RetryEvery*time.Duration(held)) {
			return
		}
		sent := time.Now()
		// A resumed context from an older term must never become valid merely
		// because the same process/pod reacquires the lease.
		holder := l.Holder + "/" + newTermIdentity()
		ok, err := l.Store.Acquire(ctx, l.Name, holder, l.TTL)
		if err != nil && ctx.Err() == nil {
			l.logf("[Leader] Acquiring lease %s failed: %v", l.Name, err)
		}
		if ok && err == nil {
			l.state().Observed(false)
			t := l.lead(ctx, sent, holder, fn)
			// After a yield or release, wait so another replica can take the lease; otherwise
			// (lost, or the job returned) ask again at once.
			wait = l.state().End(t)
			if wait > 0 {
				l.logf("[Leader] %s waits %s before asking for %s again", l.Holder, wait, l.Name)
			}
			continue
		}
		if err == nil {
			l.state().Observed(true)
		}
		wait = l.RetryEvery + jitter(l.RetryEvery)
	}
}

// sleep waits d or until ctx is done; it reports whether ctx is still live.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// jitter is a random duration in [0, d).
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d)))
}

// lead runs fn until the lease is lost, the deadline passes, fn returns or ctx is done, then
// releases the lease. It returns the term, which says whether fn yielded or released.
func (l *Lease) lead(ctx context.Context, granted time.Time, holder string, fn func(ctx context.Context)) *Term {
	l.Pool.Add(1)
	defer l.Pool.Add(-1)
	l.logf("[Leader] %s acquired lease %s", l.Holder, l.Name)
	termCtx, term := l.state().Begin(ctx, l.Pool)
	leaderCtx, cancel := context.WithCancel(termCtx)
	if store, ok := l.Store.(FencingStore); ok {
		token, err := store.Fence(ctx, l.Name, holder)
		if err != nil {
			cancel()
			l.logf("[Leader] Cannot activate fenced term %s: %v", l.Name, err)
			releaseCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
			_ = l.Store.Release(releaseCtx, l.Name, holder)
			stop()
			return term
		}
		leaderCtx = WithAuthority(leaderCtx, token, store)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(leaderCtx)
	}()

	deadline := time.NewTimer(time.Until(granted.Add(l.TTL - l.Margin)))
	defer deadline.Stop()
	renew := time.NewTicker(l.RenewEvery)
	defer renew.Stop()

	reason := ""
	for reason == "" {
		select {
		case <-ctx.Done():
			reason = "shutting down"
		case <-done:
			reason = "leader work returned"
		case <-deadline.C:
			reason = "lease deadline passed without a renewal"
		case <-renew.C:
			sent := time.Now()
			renewCtx, renewCancel := context.WithDeadline(ctx, granted.Add(l.TTL-l.Margin))
			ok, err := l.Store.Renew(renewCtx, l.Name, holder, l.TTL)
			if err == nil && ok {
				if store, supported := l.Store.(FencingStore); supported {
					_, err = store.Fence(renewCtx, l.Name, holder)
				}
			}
			renewCancel()
			switch {
			case err == nil && ok:
				granted = sent
				deadline.Reset(time.Until(granted.Add(l.TTL - l.Margin)))
			case err == nil:
				reason = "lease taken by another holder"
			default:
				// Keep leading until the deadline; the next tick retries.
				if ctx.Err() == nil {
					l.logf("[Leader] Renewing lease %s failed (retrying until the deadline): %v", l.Name, err)
				}
			}
		}
	}
	cancel()
	<-done

	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer releaseCancel()
	if err := l.Store.Release(releaseCtx, l.Name, holder); err != nil {
		l.logf("[Leader] Releasing lease %s failed (it expires on its own): %v", l.Name, err)
	}
	if outcome := term.Outcome(); outcome != "" {
		reason = outcome
	}
	l.logf("[Leader] %s stopped leading %s: %s", l.Holder, l.Name, reason)
	return term
}
