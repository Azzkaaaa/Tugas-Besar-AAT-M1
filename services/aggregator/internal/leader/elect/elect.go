// Package elect builds the leader electors selected by configuration (LEADER_ELECTION): one
// lease per leader-only job.
package elect

import (
	"fmt"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader/kubernetes"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/open"
)

// Leader-only jobs. Each has its own lease ("<LEADER_LEASE_PREFIX>-<job>"), so it fails over on
// its own and may lead on a different replica than the others.
const (
	JobBMKGPoller      = "bmkg-poller"
	JobPVMBGPoller     = "pvmbg-poller"
	JobOutboxPruner    = "outbox-pruner"
	JobOutboxPublisher = "outbox-publisher"
)

// Open returns the configured electors. The postgres and cassandra electors use the canonical
// store's lease table, so they need that store backend. All electors of the process share one
// leader.Pool, which spreads jobs across replicas.
func Open(cfg *config.Config, store *open.Store) (leader.Electors, error) {
	lc := cfg.Leader
	if cfg.RequireWriteAuthority {
		if lc.Election != cfg.CanonicalStoreBackend {
			return nil, fmt.Errorf("M2 write authority requires LEADER_ELECTION=%s", cfg.CanonicalStoreBackend)
		}
		if store == nil {
			return nil, fmt.Errorf("M2 write authority requires a canonical lease store")
		}
		if _, ok := store.Leases.(leader.FencingStore); !ok {
			return nil, fmt.Errorf("canonical lease store does not enforce M2 write authority")
		}
	}
	identity := lc.Identity
	if identity == "" {
		identity = leader.Identity()
	}
	pool := &leader.Pool{}
	yield := &leader.YieldConfig{
		PerHour: lc.YieldPerHour, Growth: lc.YieldGrowth, YieldCooldown: lc.YieldCooldown,
		ReleaseCooldown: lc.ReleaseCooldown, MaxReleaseCooldown: lc.ReleaseMaxCooldown,
	}
	if err := yield.Validate(); err != nil {
		return nil, err
	}
	switch lc.Election {
	case "single":
		return leader.SingleElectors{}, nil
	case "postgres", "cassandra":
		if lc.Election != cfg.CanonicalStoreBackend {
			return nil, fmt.Errorf("LEADER_ELECTION=%s needs CANONICAL_STORE_BACKEND=%s (it uses the store's lease table), got %s",
				lc.Election, lc.Election, cfg.CanonicalStoreBackend)
		}
		e := &leader.LeaseElectors{
			Store: store.Leases, Prefix: lc.LeasePrefix, Holder: identity,
			TTL: lc.TTL, RenewEvery: lc.RenewEvery, RetryEvery: lc.RetryEvery, Margin: lc.Margin, Pool: pool,
			Yield: yield,
		}
		if err := e.Validate(); err != nil {
			return nil, err
		}
		return e, nil
	case "kubernetes":
		return kubernetes.NewFromEnvironment(kubernetes.Config{
			Namespace: lc.K8sNamespace, Identity: identity,
			TTL: lc.TTL, RenewEvery: lc.RenewEvery, Margin: lc.Margin, Pool: pool, Yield: yield,
		}, lc.LeasePrefix)
	}
	return nil, fmt.Errorf("unknown leader election %q", lc.Election)
}
