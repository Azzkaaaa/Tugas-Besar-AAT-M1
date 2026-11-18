// Package open opens the canonical store backend selected by configuration.
package open

import (
	"context"
	"fmt"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/cassandra"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/postgres"
)

// Store is an opened backend plus its lease store for leader election.
type Store struct {
	*storage.Backend
	// Leases is the backend's leader.LeaseStore (used by the postgres and cassandra electors).
	Leases leader.LeaseStore
}

// CassandraConfig maps the configuration to the backend's. The tools in cmd/ use it too.
func CassandraConfig(c config.CassandraConfig, outboxRetention time.Duration) cassandra.Config {
	return cassandra.Config{
		Hosts:           c.Hosts,
		Keyspace:        c.Keyspace,
		Username:        c.Username,
		Password:        c.Password,
		NodeFromRack:    c.NodeFromRack,
		Consistency:     c.Consistency,
		LocalDatacenter: c.LocalDatacenter,
		Replication: cassandra.Replication{
			Strategy:    c.ReplicationStrategy,
			Factor:      c.ReplicationFactor,
			Datacenters: c.Datacenters,
		},
		BucketPeriod:    c.BucketPeriod,
		OutboxRetention: outboxRetention,
	}
}

func Open(ctx context.Context, cfg *config.Config) (*Store, error) {
	switch cfg.CanonicalStoreBackend {
	case "postgres":
		db, err := postgres.Connect(ctx, cfg.CanonicalStoreDSN)
		if err != nil {
			return nil, err
		}
		return &Store{Backend: db.Backend(), Leases: db.Leases()}, nil
	case "cassandra":
		db, err := cassandra.Open(ctx, CassandraConfig(cfg.Cassandra, cfg.OutboxRetention))
		if err != nil {
			return nil, err
		}
		return &Store{Backend: db.Backend(), Leases: db.Leases()}, nil
	}
	return nil, fmt.Errorf("unknown canonical store backend %q", cfg.CanonicalStoreBackend)
}
