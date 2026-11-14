package leader

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Cooperative release. A leader job may give its lease up at a safe point of its own choosing:
//
//   - MaybeYield: rebalancing. A replica holding h leases yields one at rate
//     PerHour × (Growth^(h−1) − 1) / (Growth − 1) per hour, so never its last lease (h = 1) and
//     much more often when it holds many. Each replica only knows its own count; with the spreading
//     delay of Lease.Run, yielded leases tend to move to replicas holding fewer, so placement
//     drifts toward even without replicas knowing about each other.
//   - Release: intentional hand-off, e.g. a poller that cannot reach its upstream from here.
//
// After either, the replica waits a cooldown before asking for that lease again, so another
// replica can take it. If nobody did (the lease was still free when the cooldown ended), a yield
// was fruitless — this replica is probably alone — so yields of that job become rarer (halved,
// recovering ×2 per hour). Release cooldowns double with every release (up to
// MaxReleaseCooldown) whether or not the lease was taken: when every replica fails to reach the
// upstream, each takes the poller, fails and hands it back, so "taken" says nothing about
// success. The cooldown resets only after this replica has led the job for MaxReleaseCooldown
// without releasing it.

// YieldConfig tunes cooperative release.
type YieldConfig struct {
	// PerHour is the yield rate of one job at h = 2 leases held; 0 disables yielding.
	PerHour float64
	// Growth is the rate ratio per extra lease held (≥ 1).
	Growth float64
	// YieldCooldown is the wait after a yield before asking for the lease again.
	YieldCooldown time.Duration
	// ReleaseCooldown is the first wait after a release; MaxReleaseCooldown caps its doubling.
	ReleaseCooldown    time.Duration
	MaxReleaseCooldown time.Duration
}

// Validate checks c.
func (c *YieldConfig) Validate() error {
	switch {
	case c.PerHour < 0:
		return errors.New("yield rate must not be negative")
	case c.Growth < 1:
		return errors.New("yield growth must be at least 1")
	case c.YieldCooldown < 0 || c.ReleaseCooldown < 0 || c.MaxReleaseCooldown < c.ReleaseCooldown:
		return errors.New("yield/release cooldowns must not be negative, and the maximum release cooldown must be at least the release cooldown")
	}
	return nil
}

// rate is the per-hour yield rate of one job when the process holds held leases.
func (c *YieldConfig) rate(held int) float64 {
	if c == nil || c.PerHour <= 0 || held <= 1 {
		return 0
	}
	steps := float64(held - 1)
	if c.Growth != 1 {
		steps = (math.Pow(c.Growth, float64(held-1)) - 1) / (c.Growth - 1)
	}
	return c.PerHour * steps
}

const (
	minBackoff = 1.0 / 64
	// backoffRecovery is how long a halving of the yield rate lasts.
	backoffRecovery = time.Hour
)

type handoffKind int

const (
	kept handoffKind = iota
	yielded
	released
)

// Term is one leadership of one job, carried in the job's context.
type Term struct {
	pool    *Pool
	cfg     *YieldConfig
	backoff float64
	rand    func() float64

	mu      sync.Mutex
	started time.Time
	last    time.Time
	outcome handoffKind
	reason  string
}

type termKey struct{}

// Outcome describes how the term ended for logs ("" when the job kept the lease).
func (t *Term) Outcome() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch t.outcome {
	case yielded:
		return "yielded the lease (rebalancing)"
	case released:
		return "released the lease: " + t.reason
	}
	return ""
}

// MaybeYield is called by a leader job at a safe point. True means the job should return now:
// its lease is handed over (see the package comment). It is false outside a lease term.
func MaybeYield(ctx context.Context) bool {
	t, _ := ctx.Value(termKey{}).(*Term)
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.outcome != kept {
		return true
	}
	now := time.Now()
	elapsed := now.Sub(t.last)
	t.last = now
	lambda := t.cfg.rate(t.pool.Held()) * t.backoff
	if lambda <= 0 || elapsed <= 0 {
		return false
	}
	if t.rand() < yieldProbability(lambda, elapsed) {
		t.outcome = yielded
		return true
	}
	return false
}

// Release is called by a leader job that should not lead here (reason says why). True means the
// job should return now and its lease is handed over; false (no lease term, e.g. the single
// elector) means there is nobody to hand it to, so the job carries on.
func Release(ctx context.Context, reason string) bool {
	t, _ := ctx.Value(termKey{}).(*Term)
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.outcome == kept {
		t.outcome, t.reason = released, reason
	}
	return true
}

// yieldProbability is the chance of yielding at a call elapsed after the previous one, at lambda
// yields per hour. It is time-normalized (a Poisson process): the chance of not having yielded
// after a stretch of time is exp(−λ·t) however often the job checks.
func yieldProbability(lambda float64, elapsed time.Duration) float64 {
	if lambda <= 0 || elapsed <= 0 {
		return 0
	}
	return 1 - math.Exp(-lambda*elapsed.Hours())
}

// Handoff keeps one job's cooperative-release state across its terms on this process. Electors
// create one per job.
type Handoff struct {
	cfg  *YieldConfig
	rand func() float64

	mu          sync.Mutex
	backoff     float64
	recoveredAt time.Time
	releaseWait time.Duration
	pending     handoffKind // last hand-off, until we learn whether someone took the lease
}

// NewHandoff returns the state of one job; a nil cfg disables yielding (Release still works,
// with no cooldown).
func NewHandoff(cfg *YieldConfig) *Handoff {
	h := &Handoff{cfg: cfg, rand: rand.Float64, backoff: 1, recoveredAt: time.Now()}
	if cfg != nil {
		h.releaseWait = cfg.ReleaseCooldown
	}
	return h
}

// Begin starts a term: the returned context carries it to the job.
func (h *Handoff) Begin(ctx context.Context, pool *Pool) (context.Context, *Term) {
	h.mu.Lock()
	// The yield rate recovers ×2 per backoffRecovery since the last change, back to 1.
	if h.backoff < 1 {
		steps := time.Since(h.recoveredAt) / backoffRecovery
		for ; steps > 0 && h.backoff < 1; steps-- {
			h.backoff = math.Min(1, h.backoff*2)
			h.recoveredAt = h.recoveredAt.Add(backoffRecovery)
		}
	}
	t := &Term{pool: pool, cfg: h.cfg, backoff: h.backoff, rand: h.rand, last: time.Now(), started: time.Now()}
	h.mu.Unlock()
	return context.WithValue(ctx, termKey{}, t), t
}

// End records how t ended and returns how long to wait before asking for the lease again.
func (h *Handoff) End(t *Term) time.Duration {
	t.mu.Lock()
	outcome, led := t.outcome, time.Since(t.started)
	t.mu.Unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pending = outcome
	switch {
	case h.cfg == nil:
		return 0
	case outcome == yielded:
		return h.cfg.YieldCooldown
	case outcome == released:
		wait := h.releaseWait
		h.releaseWait = min(2*h.releaseWait, h.cfg.MaxReleaseCooldown)
		return wait
	case led >= h.cfg.MaxReleaseCooldown:
		h.releaseWait = h.cfg.ReleaseCooldown // a healthy term: forget earlier releases
	}
	return 0
}

// Observed reports the next campaign after a hand-off: taken is true when another holder had the
// lease, false when this process got it straight back (nobody took it). A yield nobody took
// halves that job's yield rate.
func (h *Handoff) Observed(taken bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg != nil && !taken && h.pending == yielded {
		h.backoff = math.Max(minBackoff, h.backoff/2)
		h.recoveredAt = time.Now()
	}
	h.pending = kept
}
