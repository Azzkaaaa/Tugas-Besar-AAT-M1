// Package open opens the canonical store backend selected by configuration.
package open

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/cassandra"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/postgres"
)

// Store is an opened backend plus how to run the single-replica ingest work on it.
type Store struct {
	*storage.Backend
	// RunIngest runs fn on one replica at a time where the backend can elect one, and directly
	// otherwise. It returns once ctx is done.
	//
	// TODO(leader rework): leader election is being reworked separately. Until then Postgres
	// keeps its advisory-lock leader and Cassandra has none, so a Cassandra deployment must run
	// a single aggregator replica.
	RunIngest func(ctx context.Context, fn func(ctx context.Context))
}

// CassandraConfig maps the configuration to the backend's. The tools in cmd/ use it too.
func CassandraConfig(c config.CassandraConfig, outboxRetention time.Duration) cassandra.Config {
	return cassandra.Config{
		Hosts:           c.Hosts,
		Keyspace:        c.Keyspace,
		Username:        c.Username,
		Password:        c.Password,
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
		return &Store{Backend: db.Backend(), RunIngest: db.RunAsIngestLeader}, nil
	case "cassandra":
		db, err := cassandra.Open(ctx, CassandraConfig(cfg.Cassandra, cfg.OutboxRetention))
		if err != nil {
			return nil, err
		}
		return &Store{Backend: db.Backend(), RunIngest: func(ctx context.Context, fn func(ctx context.Context)) {
			log.Println("[Aggregator] Cassandra store has no ingest leader: run a single aggregator replica")
			fn(ctx)
		}}, nil
	}
	return nil, fmt.Errorf("unknown canonical store backend %q", cfg.CanonicalStoreBackend)
}
