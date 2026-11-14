package leader

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memStore is an in-memory LeaseStore. failRenew makes renewals error (an unreachable store).
type memStore struct {
	mu        sync.Mutex
	holder    string
	expires   time.Time
	failRenew atomic.Bool
	released  atomic.Int32
}

func (m *memStore) Acquire(_ context.Context, _, holder string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.holder != "" && m.holder != holder && time.Now().Before(m.expires) {
		return false, nil
	}
	m.holder, m.expires = holder, time.Now().Add(ttl)
	return true, nil
}

func (m *memStore) Renew(_ context.Context, _, holder string, ttl time.Duration) (bool, error) {
	if m.failRenew.Load() {
		return false, errors.New("store unreachable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.holder != holder || !time.Now().Before(m.expires) {
		return false, nil
	}
	m.expires = time.Now().Add(ttl)
	return true, nil
}

func (m *memStore) Release(_ context.Context, _, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.holder == holder {
		m.holder = ""
		m.released.Add(1)
	}
	return nil
}

func (m *memStore) steal(holder string, ttl time.Duration) {
	m.mu.Lock()
	m.holder, m.expires = holder, time.Now().Add(ttl)
	m.mu.Unlock()
}

func newLease(store LeaseStore, holder string) *Lease {
	return &Lease{
		Store: store, Name: "test", Holder: holder,
		TTL: 600 * time.Millisecond, RenewEvery: 100 * time.Millisecond,
		RetryEvery: 50 * time.Millisecond, Margin: 200 * time.Millisecond,
		Logf: func(string, ...any) {},
	}
}

// tracker counts concurrently running leaders and how often each started.
type tracker struct {
	running, maxRunning atomic.Int32
	starts              sync.Map // holder -> *atomic.Int32
}

func (tr *tracker) work(holder string) func(ctx context.Context) {
	return func(ctx context.Context) {
		n, _ := tr.starts.LoadOrStore(holder, new(atomic.Int32))
		n.(*atomic.Int32).Add(1)
		cur := tr.running.Add(1)
		for {
			m := tr.maxRunning.Load()
			if cur <= m || tr.maxRunning.CompareAndSwap(m, cur) {
				break
			}
		}
		<-ctx.Done()
		tr.running.Add(-1)
	}
}

func (tr *tracker) startsOf(holder string) int32 {
	n, ok := tr.starts.Load(holder)
	if !ok {
		return 0
	}
	return n.(*atomic.Int32).Load()
}

func TestLeaseValidate(t *testing.T) {
	l := newLease(&memStore{}, "a")
	if err := l.Validate(); err != nil {
		t.Fatal(err)
	}
	l.RenewEvery = l.TTL - l.Margin
	if err := l.Validate(); err == nil {
		t.Fatal("renew interval not shorter than TTL - margin accepted")
	}
}

func TestLeaseExclusiveAndTakeover(t *testing.T) {
	store := &memStore{}
	tr := &tracker{}
	ctxA, stopA := context.WithCancel(context.Background())
	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	doneA := make(chan struct{})
	go func() { newLease(store, "a").Run(ctxA, tr.work("a")); close(doneA) }()
	time.Sleep(100 * time.Millisecond)
	go newLease(store, "b").Run(ctxB, tr.work("b"))

	time.Sleep(time.Second) // several renewals
	if tr.startsOf("a") != 1 || tr.startsOf("b") != 0 {
		t.Fatalf("starts a=%d b=%d, want a only", tr.startsOf("a"), tr.startsOf("b"))
	}
	stopA()
	<-doneA
	if store.released.Load() != 1 {
		t.Fatal("stopping leader did not release the lease")
	}
	time.Sleep(300 * time.Millisecond) // b retries every 50ms
	if tr.startsOf("b") != 1 {
		t.Fatal("b did not take over after a released")
	}
	if tr.maxRunning.Load() != 1 {
		t.Fatalf("%d leaders ran at once", tr.maxRunning.Load())
	}
}

// When the store becomes unreachable the leader stops itself at the local deadline, before the
// lease can expire in the store and another replica take over.
func TestLeaseSelfFencesBeforeExpiry(t *testing.T) {
	store := &memStore{}
	l := newLease(store, "a")
	stopped := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx, func(ctx context.Context) {
		<-ctx.Done()
		stopped <- time.Now()
	})
	time.Sleep(250 * time.Millisecond)
	store.failRenew.Store(true)
	failedAt := time.Now()

	select {
	case at := <-stopped:
		store.mu.Lock()
		expires := store.expires
		store.mu.Unlock()
		if !at.Before(expires) {
			t.Fatalf("leader stopped at %s, after the lease expired at %s", at, expires)
		}
		if at.Sub(failedAt) > l.TTL {
			t.Fatalf("leader kept running %s after renewals failed", at.Sub(failedAt))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not stop when renewals kept failing")
	}
}

func TestLeaseStopsWhenTaken(t *testing.T) {
	store := &memStore{}
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	go newLease(store, "a").Run(ctx, func(ctx context.Context) {
		<-ctx.Done()
		once.Do(func() { close(stopped) })
	})
	time.Sleep(150 * time.Millisecond)
	store.steal("other", time.Hour)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("leader did not stop after its lease was taken")
	}
}

func TestLeaseRestartsReturnedWork(t *testing.T) {
	store := &memStore{}
	var runs atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	newLease(store, "a").Run(ctx, func(context.Context) { runs.Add(1); time.Sleep(50 * time.Millisecond) })
	if runs.Load() < 3 {
		t.Fatalf("returned work restarted %d times", runs.Load())
	}
}

func TestSingle(t *testing.T) {
	var ran atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	Single{}.Run(ctx, func(ctx context.Context) { ran.Store(true); <-ctx.Done() })
	if !ran.Load() {
		t.Fatal("single elector did not run the work")
	}
}
