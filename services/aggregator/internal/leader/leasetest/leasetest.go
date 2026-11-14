// Package leasetest is the behavioural contract of leader.LeaseStore implementations, run by
// each storage backend's integration tests.
package leasetest

import (
	"context"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
)

// Run checks store with a lease of ttl. name must be unused.
func Run(t *testing.T, store leader.LeaseStore, name string, ttl time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	must := func(ok bool, err error, want bool, what string) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if ok != want {
			t.Fatalf("%s = %v, want %v", what, ok, want)
		}
	}

	ok, err := store.Acquire(ctx, name, "a", ttl)
	must(ok, err, true, "a acquires free lease")
	ok, err = store.Acquire(ctx, name, "b", ttl)
	must(ok, err, false, "b acquires held lease")
	ok, err = store.Acquire(ctx, name, "a", ttl)
	must(ok, err, true, "a re-acquires its own lease")
	ok, err = store.Renew(ctx, name, "a", ttl)
	must(ok, err, true, "a renews")
	ok, err = store.Renew(ctx, name, "b", ttl)
	must(ok, err, false, "b renews a's lease")

	if err := store.Release(ctx, name, "b"); err != nil {
		t.Fatal(err)
	}
	ok, err = store.Acquire(ctx, name, "b", ttl)
	must(ok, err, false, "b acquires after releasing a lease it does not hold")
	if err := store.Release(ctx, name, "a"); err != nil {
		t.Fatal(err)
	}
	ok, err = store.Acquire(ctx, name, "b", ttl)
	must(ok, err, true, "b acquires after a released")

	// Expiry: b stops renewing, a takes over after the TTL, and b's renewal then fails.
	time.Sleep(ttl + 500*time.Millisecond)
	ok, err = store.Acquire(ctx, name, "a", ttl)
	must(ok, err, true, "a acquires expired lease")
	ok, err = store.Renew(ctx, name, "b", ttl)
	must(ok, err, false, "b renews after expiry")
	_ = store.Release(ctx, name, "a")
}
