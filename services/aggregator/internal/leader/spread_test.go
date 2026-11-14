package leader

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mapStore is an in-memory LeaseStore with any number of named leases.
type mapStore struct {
	mu     sync.Mutex
	leases map[string]*memLease
}

type memLease struct {
	holder  string
	expires time.Time
}

func newMapStore() *mapStore { return &mapStore{leases: map[string]*memLease{}} }

func (m *mapStore) Acquire(_ context.Context, name, holder string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.leases[name]
	if l != nil && l.holder != holder && time.Now().Before(l.expires) {
		return false, nil
	}
	m.leases[name] = &memLease{holder, time.Now().Add(ttl)}
	return true, nil
}

func (m *mapStore) Renew(_ context.Context, name, holder string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.leases[name]
	if l == nil || l.holder != holder || !time.Now().Before(l.expires) {
		return false, nil
	}
	l.expires = time.Now().Add(ttl)
	return true, nil
}

func (m *mapStore) Release(_ context.Context, name, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l := m.leases[name]; l != nil && l.holder == holder {
		delete(m.leases, name)
	}
	return nil
}

var testJobs = []string{"bmkg-poller", "pvmbg-poller", "outbox-pruner", "outbox-publisher"}

// leaders records which replica currently leads each job and fails on a double leader.
type leaders struct {
	mu      sync.Mutex
	current map[string]string
	double  atomic.Bool
}

func (ls *leaders) work(replica, job string) func(ctx context.Context) {
	return func(ctx context.Context) {
		ls.mu.Lock()
		if ls.current[job] != "" {
			ls.double.Store(true)
		}
		ls.current[job] = replica
		ls.mu.Unlock()
		<-ctx.Done()
		ls.mu.Lock()
		if ls.current[job] == replica {
			delete(ls.current, job)
		}
		ls.mu.Unlock()
	}
}

func (ls *leaders) count(replica string) (n int) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	for _, r := range ls.current {
		if r == replica {
			n++
		}
	}
	return n
}

func (ls *leaders) led() int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return len(ls.current)
}

// replica runs every job under its own lease, as cmd/main.go does, and returns its pool and a
// stop function that waits for the electors to return.
func replica(t *testing.T, store LeaseStore, name string, ls *leaders) (*Pool, func()) {
	t.Helper()
	pool := &Pool{}
	electors := &LeaseElectors{Store: store, Prefix: "agg", Holder: name,
		TTL: 600 * time.Millisecond, RenewEvery: 100 * time.Millisecond,
		RetryEvery: 100 * time.Millisecond, Margin: 200 * time.Millisecond, Pool: pool}
	if err := electors.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, job := range testJobs {
		e := electors.For(job).(*Lease)
		e.Logf = func(string, ...any) {}
		wg.Add(1)
		go func() { defer wg.Done(); e.Run(ctx, ls.work(name, job)) }()
	}
	return pool, func() { cancel(); wg.Wait() }
}

func eventually(t *testing.T, within time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestLeaseElectorsNames(t *testing.T) {
	e := &LeaseElectors{Prefix: "aggregator"}
	if got := e.For("bmkg-poller").(*Lease).Name; got != "aggregator-bmkg-poller" {
		t.Fatalf("lease name %q", got)
	}
	if _, ok := (SingleElectors{}).For("x").(Single); !ok {
		t.Fatal("SingleElectors did not give Single")
	}
}

func TestLoneReplicaLeadsEveryJob(t *testing.T) {
	ls := &leaders{current: map[string]string{}}
	pool, stop := replica(t, newMapStore(), "a", ls)
	eventually(t, time.Second, func() bool { return ls.count("a") == len(testJobs) }, "lone replica did not lead every job")
	if pool.Held() != len(testJobs) {
		t.Fatalf("pool holds %d, want %d", pool.Held(), len(testJobs))
	}
	stop()
	if pool.Held() != 0 {
		t.Fatalf("pool holds %d after shutdown", pool.Held())
	}
}

// Started together, two replicas share the jobs; when one stops, the other takes them all.
func TestJobsSpreadAndFailOver(t *testing.T) {
	// Placement is random: require a split in most of several runs, never a double leader.
	split := 0
	const runs = 5
	for run := 0; run < runs; run++ {
		store := newMapStore()
		ls := &leaders{current: map[string]string{}}
		_, stopA := replica(t, store, "a", ls)
		poolB, stopB := replica(t, store, "b", ls)
		eventually(t, 2*time.Second, func() bool { return ls.led() == len(testJobs) }, "not every job got a leader")
		time.Sleep(500 * time.Millisecond) // let late attempts settle
		t.Logf("run %d: a=%d b=%d", run, ls.count("a"), ls.count("b"))
		if ls.count("a") > 0 && ls.count("b") > 0 {
			split++
		}

		stopA()
		eventually(t, 3*time.Second, func() bool { return ls.count("b") == len(testJobs) },
			"b did not take over every job after a stopped")
		if poolB.Held() != len(testJobs) {
			t.Fatalf("b's pool holds %d, want %d", poolB.Held(), len(testJobs))
		}
		stopB()
		if ls.double.Load() {
			t.Fatal("a job had two leaders at once")
		}
	}
	if split < runs-1 {
		t.Fatalf("jobs split between replicas in only %d of %d runs", split, runs)
	}
}
