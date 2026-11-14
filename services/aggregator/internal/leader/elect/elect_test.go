package elect

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/open"
)

func cfg(election, backend string) *config.Config {
	return &config.Config{CanonicalStoreBackend: backend, Leader: config.LeaderConfig{
		Election: election, TTL: 15 * time.Second, RenewEvery: 5 * time.Second, RetryEvery: 5 * time.Second, Margin: 5 * time.Second,
	}}
}

func TestOpen(t *testing.T) {
	if e, err := Open(cfg("single", "cassandra"), store()); err != nil {
		t.Fatal(err)
	} else if _, ok := e.(leader.Single); !ok {
		t.Fatalf("single gave %T", e)
	}
	if e, err := Open(cfg("cassandra", "cassandra"), store()); err != nil {
		t.Fatal(err)
	} else if l, ok := e.(*leader.Lease); !ok || l.Name != LeaseName || l.Holder == "" {
		t.Fatalf("cassandra gave %#v", e)
	}
	if _, err := Open(cfg("postgres", "cassandra"), store()); err == nil || !strings.Contains(err.Error(), "CANONICAL_STORE_BACKEND") {
		t.Fatalf("postgres elector on cassandra store: %v", err)
	}
	bad := cfg("postgres", "postgres")
	bad.Leader.RenewEvery = 20 * time.Second
	if _, err := Open(bad, store()); err == nil {
		t.Fatal("invalid lease timing accepted")
	}
}

type nopLeases struct{}

func (nopLeases) Acquire(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}
func (nopLeases) Renew(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}
func (nopLeases) Release(context.Context, string, string) error { return nil }

func store() *open.Store { return &open.Store{Leases: nopLeases{}} }
