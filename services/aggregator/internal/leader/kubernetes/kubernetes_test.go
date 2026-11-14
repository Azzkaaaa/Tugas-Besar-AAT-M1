package kubernetes

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func newTestElector(t *testing.T, client *fake.Clientset, id string) *Elector {
	t.Helper()
	e, err := New(Config{
		Client: client, Namespace: "bnpb", LeaseName: "aggregator-ingest", Identity: id,
		TTL: 2 * time.Second, RenewEvery: 200 * time.Millisecond, Margin: time.Second,
		Logf: func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestNewValidatesTiming(t *testing.T) {
	_, err := New(Config{Client: fake.NewClientset(), Namespace: "n", LeaseName: "l", Identity: "a",
		TTL: 2 * time.Second, RenewEvery: time.Second, Margin: time.Second})
	if err == nil {
		t.Fatal("renew deadline not above 1.2 x retry period accepted")
	}
}

func TestKubernetesLeaseExclusiveAndTakeover(t *testing.T) {
	client := fake.NewClientset()
	var running, maxRunning, startsA, startsB atomic.Int32
	work := func(starts *atomic.Int32) func(ctx context.Context) {
		return func(ctx context.Context) {
			starts.Add(1)
			n := running.Add(1)
			if n > maxRunning.Load() {
				maxRunning.Store(n)
			}
			<-ctx.Done()
			running.Add(-1)
		}
	}

	ctxA, stopA := context.WithCancel(context.Background())
	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	doneA := make(chan struct{})
	go func() { newTestElector(t, client, "a").Run(ctxA, work(&startsA)); close(doneA) }()
	waitFor(t, func() bool { return startsA.Load() == 1 }, 3*time.Second, "a did not lead")
	go newTestElector(t, client, "b").Run(ctxB, work(&startsB))

	time.Sleep(time.Second)
	if startsB.Load() != 0 {
		t.Fatal("b led while a held the lease")
	}
	stopA()
	<-doneA
	// a released the lease on shutdown, so b takes over without waiting for the TTL.
	waitFor(t, func() bool { return startsB.Load() == 1 }, 2*time.Second, "b did not take over")
	if maxRunning.Load() != 1 {
		t.Fatalf("%d leaders ran at once", maxRunning.Load())
	}
}

func waitFor(t *testing.T, cond func() bool, within time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Two replicas with one Lease per job: every job has one leader at a time, the jobs end up on
// both replicas, and when one replica stops the other takes all of them.
func TestKubernetesJobsSpreadAndFailOver(t *testing.T) {
	jobs := []string{"bmkg-poller", "pvmbg-poller", "outbox-pruner", "outbox-publisher"}
	client := fake.NewClientset()
	var mu sync.Mutex
	current := map[string]string{}
	var double atomic.Bool
	count := func(id string) (n int) {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range current {
			if r == id {
				n++
			}
		}
		return n
	}
	start := func(id string) func() {
		electors, err := NewElectors(Config{Client: client, Namespace: "bnpb", Identity: id,
			TTL: 2 * time.Second, RenewEvery: 200 * time.Millisecond, Margin: time.Second,
			Logf: func(string, ...any) {}}, "aggregator")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for _, job := range jobs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				electors.For(job).Run(ctx, func(ctx context.Context) {
					mu.Lock()
					if current[job] != "" {
						double.Store(true)
					}
					current[job] = id
					mu.Unlock()
					<-ctx.Done()
					mu.Lock()
					delete(current, job)
					mu.Unlock()
				})
			}()
		}
		return func() { cancel(); wg.Wait() }
	}

	stopA := start("a")
	stopB := start("b")
	defer stopB()
	waitFor(t, func() bool { return count("a")+count("b") == len(jobs) }, 5*time.Second, "not every job got a leader")
	time.Sleep(time.Second)
	t.Logf("placement: a=%d b=%d", count("a"), count("b"))
	if count("a") == 0 || count("b") == 0 {
		t.Logf("jobs did not split this run (placement is random)")
	}
	stopA()
	waitFor(t, func() bool { return count("b") == len(jobs) }, 6*time.Second, "b did not take over every job")
	if double.Load() {
		t.Fatal("a job had two leaders at once")
	}
	leases, err := client.CoordinationV1().Leases("bnpb").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(leases.Items) != len(jobs) {
		t.Fatalf("leases %v, %v; want one per job", leases, err)
	}
}

// A job that releases its Lease hands it to the other replica, which leads while the releasing
// replica waits out its cooldown.
func TestKubernetesReleaseHandsOver(t *testing.T) {
	client := fake.NewClientset()
	yield := &leader.YieldConfig{Growth: 1, ReleaseCooldown: 3 * time.Second, MaxReleaseCooldown: 3 * time.Second}
	newE := func(id string) *Elector {
		e, err := New(Config{Client: client, Namespace: "bnpb", LeaseName: "aggregator-job", Identity: id,
			TTL: 2 * time.Second, RenewEvery: 200 * time.Millisecond, Margin: time.Second, Yield: yield,
			Pool: &leader.Pool{}, Logf: func(string, ...any) {}})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var aRuns, bRuns atomic.Int32
	go newE("a").Run(ctx, func(ctx context.Context) {
		aRuns.Add(1)
		leader.Release(ctx, "cannot reach upstream")
	})
	waitFor(t, func() bool { return aRuns.Load() == 1 }, 3*time.Second, "a never led")
	go newE("b").Run(ctx, func(ctx context.Context) { bRuns.Add(1); <-ctx.Done() })
	waitFor(t, func() bool { return bRuns.Load() == 1 }, 3*time.Second, "b did not take the released lease")
	if aRuns.Load() != 1 {
		t.Fatalf("a led %d times during its release cooldown", aRuns.Load())
	}
}
