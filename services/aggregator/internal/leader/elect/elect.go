// Package elect builds the leader elector selected by configuration (LEADER_ELECTION).
package elect

import (
	"fmt"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader/kubernetes"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/open"
)

// LeaseName names the ingest leader's lease in the store-backed electors.
const LeaseName = "aggregator-ingest"

// Open returns the configured elector. The postgres and cassandra electors use the canonical
// store's lease table, so they need that store backend.
func Open(cfg *config.Config, store *open.Store) (leader.Elector, error) {
	lc := cfg.Leader
	identity := lc.Identity
	if identity == "" {
		identity = leader.Identity()
	}
	switch lc.Election {
	case "single":
		return leader.Single{}, nil
	case "postgres", "cassandra":
		if lc.Election != cfg.CanonicalStoreBackend {
			return nil, fmt.Errorf("LEADER_ELECTION=%s needs CANONICAL_STORE_BACKEND=%s (it uses the store's lease table), got %s",
				lc.Election, lc.Election, cfg.CanonicalStoreBackend)
		}
		l := &leader.Lease{
			Store: store.Leases, Name: LeaseName, Holder: identity,
			TTL: lc.TTL, RenewEvery: lc.RenewEvery, RetryEvery: lc.RetryEvery, Margin: lc.Margin,
		}
		if err := l.Validate(); err != nil {
			return nil, err
		}
		return l, nil
	case "kubernetes":
		return kubernetes.NewFromEnvironment(kubernetes.Config{
			Namespace: lc.K8sNamespace, LeaseName: lc.K8sLeaseName, Identity: identity,
			TTL: lc.TTL, RenewEvery: lc.RenewEvery, Margin: lc.Margin,
		})
	}
	return nil, fmt.Errorf("unknown leader election %q", lc.Election)
}
