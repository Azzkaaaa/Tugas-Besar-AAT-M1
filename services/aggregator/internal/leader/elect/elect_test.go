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
		Election: election, LeasePrefix: "aggregator", TTL: 15 * time.Second,
		YieldPerHour: 1, YieldGrowth: 4, YieldCooldown: 5 * time.Second, ReleaseCooldown: 30 * time.Second,
		ReleaseMaxCooldown: 5 * time.Minute, RenewEvery: 5 * time.Second, RetryEvery: 5 * time.Second, Margin: 5 * time.Second,
	}}
}

func TestOpen(t *testing.T) {
	if e, err := Open(cfg("single", "cassandra"), store()); err != nil {
		t.Fatal(err)
	} else if _, ok := e.For(JobBMKGPoller).(leader.Single); !ok {
		t.Fatalf("single gave %T", e.For(JobBMKGPoller))
	}
	e, err := Open(cfg("cassandra", "cassandra"), store())
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, job := range []string{JobBMKGPoller, JobPVMBGPoller, JobOutboxPruner, JobOutboxPublisher} {
		l, ok := e.For(job).(*leader.Lease)
		if !ok || l.Holder == "" || l.Pool == nil || l.Name != "aggregator-"+job {
			t.Fatalf("cassandra elector for %s = %#v", job, e.For(job))
		}
		names[l.Name] = true
	}
	if len(names) != 4 {
		t.Fatalf("lease names not distinct: %v", names)
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

func TestM2RequiresAFencedCanonicalElector(t *testing.T) {
	for _, election := range []string{"single", "kubernetes", "cassandra"} {
		c := cfg(election, "cassandra")
		c.RequireWriteAuthority = true
		if _, err := Open(c, store()); err == nil {
			t.Fatal("unfenced M2 configuration accepted", election)
		}
	}
}
