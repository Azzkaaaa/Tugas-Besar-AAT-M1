package kubernetes

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

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
