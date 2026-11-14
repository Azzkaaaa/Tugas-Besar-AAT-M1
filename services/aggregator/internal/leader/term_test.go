package leader

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestYieldRate(t *testing.T) {
	c := &YieldConfig{PerHour: 1, Growth: 4}
	want := map[int]float64{0: 0, 1: 0, 2: 1, 3: 5, 4: 21}
	for h, w := range want {
		if got := c.rate(h); math.Abs(got-w) > 1e-9 {
			t.Errorf("rate(%d) = %v, want %v", h, got, w)
		}
	}
	if got := (&YieldConfig{PerHour: 2, Growth: 1}).rate(4); got != 6 {
		t.Errorf("linear growth rate(4) = %v, want 6", got)
	}
	if (&YieldConfig{PerHour: 0, Growth: 4}).rate(4) != 0 || (*YieldConfig)(nil).rate(4) != 0 {
		t.Error("disabled yielding has a rate")
	}
}

// The chance of keeping the lease over a stretch of time does not depend on how often the job asks.
func TestYieldProbabilityIsTimeNormalized(t *testing.T) {
	const lambda = 30.0 // per hour
	once := 1 - yieldProbability(lambda, time.Minute)
	often := 1.0
	for i := 0; i < 600; i++ {
		often *= 1 - yieldProbability(lambda, 100*time.Millisecond)
	}
	if math.Abs(once-often) > 1e-9 {
		t.Fatalf("keep probability over a minute: one call %v, 600 calls %v", once, often)
	}
}

func TestNoTermNeverYieldsOrReleases(t *testing.T) {
	if MaybeYield(context.Background()) || Release(context.Background(), "x") {
		t.Fatal("a context without a term yielded or released")
	}
	var ran atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	Single{}.Run(ctx, func(ctx context.Context) {
		ran.Store(true)
		if Release(ctx, "x") {
			t.Error("Release under the single elector reported a hand-off")
		}
	})
	if !ran.Load() {
		t.Fatal("single elector did not run the job")
	}
}

func TestHandoffCooldownsAndBackoff(t *testing.T) {
	cfg := &YieldConfig{PerHour: 1, Growth: 4, YieldCooldown: time.Second,
		ReleaseCooldown: 10 * time.Second, MaxReleaseCooldown: 35 * time.Second}
	h := NewHandoff(cfg)
	pool := &Pool{}

	release := func() time.Duration {
		ctx, term := h.Begin(context.Background(), pool)
		if !Release(ctx, "unreachable") {
			t.Fatal("Release under a term reported no hand-off")
		}
		return h.End(term)
	}
	// Every release doubles the next cooldown up to the maximum, taken or not (when every replica
	// fails, each takes the lease and hands it back).
	if got := release(); got != 10*time.Second {
		t.Fatalf("first release cooldown %s", got)
	}
	h.Observed(true)
	if got := release(); got != 20*time.Second {
		t.Fatalf("second release cooldown %s, want 20s", got)
	}
	h.Observed(false)
	if got := release(); got != 35*time.Second {
		t.Fatalf("third release cooldown %s, want capped 35s", got)
	}
	h.Observed(true)
	// A healthy term (led for MaxReleaseCooldown without releasing) resets it.
	_, healthy := h.Begin(context.Background(), pool)
	healthy.started = time.Now().Add(-time.Minute)
	if h.End(healthy) != 0 {
		t.Fatal("a kept term has a cooldown")
	}
	if got := release(); got != 10*time.Second {
		t.Fatalf("cooldown after a healthy term %s, want reset to 10s", got)
	}
	h.Observed(true)

	// A fruitless yield halves the job's yield rate; a kept term changes nothing.
	ctx, term := h.Begin(context.Background(), pool)
	term.mu.Lock()
	term.outcome = yielded
	term.mu.Unlock()
	if got := h.End(term); got != time.Second {
		t.Fatalf("yield cooldown %s", got)
	}
	h.Observed(false)
	if _, next := h.Begin(ctx, pool); next.backoff != 0.5 {
		t.Fatalf("backoff after a fruitless yield %v, want 0.5", next.backoff)
	}
	_, kept := h.Begin(ctx, pool)
	if h.End(kept) != 0 {
		t.Fatal("a term that kept its lease has a cooldown")
	}
}

// yieldingReplica runs every job under its own lease with cooperative release enabled.
func yieldingReplica(t *testing.T, store LeaseStore, name string, ls *leaders, yields *atomic.Int32, cfg *YieldConfig) (func(), []*Lease) {
	t.Helper()
	pool := &Pool{}
	electors := &LeaseElectors{Store: store, Prefix: "agg", Holder: name,
		TTL: 600 * time.Millisecond, RenewEvery: 100 * time.Millisecond,
		RetryEvery: 100 * time.Millisecond, Margin: 200 * time.Millisecond, Pool: pool, Yield: cfg}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var all []*Lease
	for _, job := range testJobs {
		e := electors.For(job).(*Lease)
		e.Logf = func(string, ...any) {}
		all = append(all, e)
		inner := ls.work(name, job)
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Run(ctx, func(ctx context.Context) {
				// A job checking for a safe point every 20ms.
				jobCtx, stop := context.WithCancel(ctx)
				defer stop()
				go func() {
					for jobCtx.Err() == nil {
						time.Sleep(20 * time.Millisecond)
						if MaybeYield(jobCtx) {
							yields.Add(1)
							stop()
						}
					}
				}()
				inner(jobCtx)
			})
		}()
	}
	return func() { cancel(); wg.Wait() }, all
}

// A replica that took most leases before a second one started gives some up: placement converges
// toward even, and no job ever has two leaders.
func TestYieldRebalancesLopsidedStart(t *testing.T) {
	cfg := &YieldConfig{PerHour: 3600 * 4, Growth: 4, YieldCooldown: 300 * time.Millisecond,
		ReleaseCooldown: time.Second, MaxReleaseCooldown: time.Second}
	store := newMapStore()
	ls := &leaders{current: map[string]string{}}
	var yields atomic.Int32
	stopA, _ := yieldingReplica(t, store, "a", ls, &yields, cfg)
	defer stopA()
	// At this test's high rate a lone replica keeps yielding, so "lopsided" means at least 3 of 4.
	eventually(t, 3*time.Second, func() bool { return ls.count("a") >= len(testJobs)-1 }, "a did not take most jobs")
	stopB, _ := yieldingReplica(t, store, "b", ls, &yields, cfg)
	defer stopB()

	// Sample placement for a few seconds: b must get work, and most samples are balanced.
	balanced, samples := 0, 0
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		a, b := ls.count("a"), ls.count("b")
		samples++
		if a >= 1 && b >= 1 {
			balanced++
		}
	}
	t.Logf("balanced (both lead) in %d of %d samples, %d yields", balanced, samples, yields.Load())
	if balanced < samples/2 {
		t.Fatalf("placement stayed lopsided: both replicas led in only %d of %d samples", balanced, samples)
	}
	if ls.double.Load() {
		t.Fatal("a job had two leaders at once")
	}
}

// Alone, every yield is fruitless (nobody takes the lease), so each job's yield rate backs off.
func TestLoneReplicaYieldsBackOff(t *testing.T) {
	cfg := &YieldConfig{PerHour: 3600 * 2, Growth: 4, YieldCooldown: 50 * time.Millisecond,
		ReleaseCooldown: time.Second, MaxReleaseCooldown: time.Second}
	ls := &leaders{current: map[string]string{}}
	var yields atomic.Int32
	stop, electors := yieldingReplica(t, newMapStore(), "a", ls, &yields, cfg)
	defer stop()
	backedOff := func() (n int) {
		for _, e := range electors {
			h := e.state()
			h.mu.Lock()
			if h.backoff < 1 {
				n++
			}
			h.mu.Unlock()
		}
		return n
	}
	eventually(t, 5*time.Second, func() bool { return backedOff() >= len(electors)-1 },
		"lone replica's jobs did not back off their yield rate")
	t.Logf("%d yields, %d of %d jobs backed off", yields.Load(), backedOff(), len(electors))
	if ls.count("a") == 0 {
		t.Fatal("lone replica stopped leading")
	}
}

// A released job moves to another replica.
func TestReleaseHandsJobOver(t *testing.T) {
	store := newMapStore()
	ls := &leaders{current: map[string]string{}}
	cfg := &YieldConfig{Growth: 1, ReleaseCooldown: 2 * time.Second, MaxReleaseCooldown: 2 * time.Second}
	newElector := func(name string) *Lease {
		return &Lease{Store: store, Name: "agg-job", Holder: name, TTL: 600 * time.Millisecond,
			RenewEvery: 100 * time.Millisecond, RetryEvery: 100 * time.Millisecond, Margin: 200 * time.Millisecond,
			Pool: &Pool{}, Yield: cfg, Logf: func(string, ...any) {}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var aRuns atomic.Int32
	go newElector("a").Run(ctx, func(ctx context.Context) {
		if aRuns.Add(1) == 1 {
			Release(ctx, "cannot reach upstream") // first term: hand it over at once
			return
		}
		ls.work("a", "job")(ctx)
	})
	eventually(t, time.Second, func() bool { return aRuns.Load() == 1 }, "a never led")
	go newElector("b").Run(ctx, ls.work("b", "job"))
	eventually(t, 2*time.Second, func() bool { return ls.count("b") == 1 }, "b did not take the released job")
	if aRuns.Load() != 1 {
		t.Fatal("a took the job back during its release cooldown")
	}
}
