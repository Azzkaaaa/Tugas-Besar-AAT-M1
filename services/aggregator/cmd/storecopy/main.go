// Command storecopy copies the canonical store from Postgres to Cassandra once: hazards, ingest
// state, unpublished outbox events and dead letters. Published outbox history is not copied.
//
// Stop the aggregator first. The copy is idempotent, so it can be re-run after a failure; run it
// before the aggregator starts ingesting into Cassandra.
//
//	go run ./cmd/storecopy \
//	  -postgres 'postgres://aggregator:...@localhost:5432/bnpb_canonical?sslmode=disable' \
//	  -cassandra localhost:9042 -keyspace bnpb_canonical -replication-factor 3
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/cassandra"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/open"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/postgres"
)

func main() {
	// Target settings come from the aggregator's CANONICAL_STORE_CASSANDRA_* environment; flags
	// override them.
	if os.Getenv("CANONICAL_STORE_CASSANDRA_HOSTS") == "" {
		_ = os.Setenv("CANONICAL_STORE_CASSANDRA_HOSTS", "localhost:9042")
	}
	c, err := config.LoadCassandra()
	if err != nil {
		log.Fatalf("configuration: %v", err)
	}

	pgDSN := flag.String("postgres", os.Getenv("CANONICAL_STORE_DSN"), "source Postgres DSN (default $CANONICAL_STORE_DSN)")
	hosts := flag.String("cassandra", strings.Join(c.Hosts, ","), "target Cassandra hosts, comma separated")
	flag.StringVar(&c.Keyspace, "keyspace", c.Keyspace, "target keyspace")
	flag.StringVar(&c.Username, "user", c.Username, "Cassandra username (password from CANONICAL_STORE_CASSANDRA_PASSWORD)")
	flag.StringVar(&c.Consistency, "consistency", c.Consistency, "Cassandra consistency")
	flag.StringVar(&c.LocalDatacenter, "local-dc", c.LocalDatacenter, "local datacenter (LOCAL_* consistency)")
	flag.StringVar(&c.ReplicationStrategy, "replication-strategy", c.ReplicationStrategy, "SimpleStrategy or NetworkTopologyStrategy, if the keyspace is created")
	flag.IntVar(&c.ReplicationFactor, "replication-factor", c.ReplicationFactor, "SimpleStrategy replication factor, if the keyspace is created")
	dcs := flag.String("datacenters", formatDatacenters(c.Datacenters), "NetworkTopologyStrategy replicas per datacenter (dc1:3,dc2:3), if the keyspace is created")
	flag.DurationVar(&c.BucketPeriod, "bucket-period", c.BucketPeriod, "initial bucket period if the keyspace is new (an existing keyspace keeps its schedule)")
	retention := flag.Duration("outbox-retention", 168*time.Hour, "outbox retention (TTL of finished events)")
	dryRun := flag.Bool("dry-run", false, "read and count only, write nothing")
	flag.Parse()

	if *pgDSN == "" {
		log.Fatal("no source: pass -postgres or set CANONICAL_STORE_DSN")
	}
	if err := cassandra.ValidateBucketPeriod(c.BucketPeriod); err != nil {
		log.Fatalf("-bucket-period: %v", err)
	}
	c.Hosts = strings.Split(*hosts, ",")
	if c.Datacenters, err = parseDatacenters(*dcs); err != nil {
		log.Fatalf("-datacenters: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	src, err := postgres.Connect(ctx, *pgDSN)
	if err != nil {
		log.Fatalf("source: %v", err)
	}
	defer src.Pool.Close()

	var dst *cassandra.DB
	if !*dryRun {
		dst, err = cassandra.Open(ctx, open.CassandraConfig(c, *retention))
		if err != nil {
			log.Fatalf("target: %v", err)
		}
		defer dst.Close()
	}

	if err := run(ctx, src, dst); err != nil {
		log.Fatal(err)
	}
	if *dryRun {
		log.Println("dry run: nothing written")
	}
}

func run(ctx context.Context, src *postgres.DB, dst *cassandra.DB) error {
	// Hazards, through the public List.
	var copied, conflicts int
	token := ""
	for {
		page, err := src.Backend().Hazards.List(ctx, storage.HazardQuery{}, storage.PageRequest{Size: 100, Token: token})
		if err != nil {
			return fmt.Errorf("read hazards: %w", err)
		}
		for _, h := range page.Items {
			if dst == nil {
				copied++
				continue
			}
			err := dst.ImportHazard(ctx, h)
			if errors.Is(err, cassandra.ErrIdentityConflict) {
				log.Printf("skip: %v", err)
				conflicts++
				continue
			}
			if err != nil {
				return fmt.Errorf("write hazard %s: %w", h.HazardID, err)
			}
			copied++
		}
		if page.NextToken == "" {
			break
		}
		token = page.NextToken
	}
	log.Printf("hazards: %d copied, %d skipped (identity conflict)", copied, conflicts)

	states, err := src.ExportSourceStates(ctx)
	if err != nil {
		return fmt.Errorf("read source state: %w", err)
	}
	for _, s := range states {
		if dst != nil {
			if err := dst.ImportSourceState(ctx, s.Key, s.State); err != nil {
				return fmt.Errorf("write source state %s: %w", s.Key, err)
			}
		}
	}
	log.Printf("source states: %d", len(states))

	pending, err := src.ExportPendingEvents(ctx)
	if err != nil {
		return fmt.Errorf("read pending outbox events: %w", err)
	}
	for _, ev := range pending {
		if dst != nil {
			if err := dst.ImportPendingEvent(ctx, ev); err != nil {
				return fmt.Errorf("write outbox event %s: %w", ev.EventID, err)
			}
		}
	}
	log.Printf("pending outbox events: %d", len(pending))

	dead, err := src.ExportDeadLetters(ctx)
	if err != nil {
		return fmt.Errorf("read dead letters: %w", err)
	}
	for _, dl := range dead {
		if dst != nil {
			if err := dst.ImportDeadLetter(ctx, &dl.Event, dl.FailedAt, dl.LastError); err != nil {
				return fmt.Errorf("write dead letter %s: %w", dl.Event.EventID, err)
			}
		}
	}
	log.Printf("dead letters: %d", len(dead))
	return nil
}

func formatDatacenters(dcs map[string]int) string {
	names := make([]string, 0, len(dcs))
	for dc := range dcs {
		names = append(names, dc)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, dc := range names {
		parts[i] = fmt.Sprintf("%s:%d", dc, dcs[dc])
	}
	return strings.Join(parts, ",")
}

// parseDatacenters parses the -datacenters flag (same syntax as CANONICAL_STORE_CASSANDRA_DATACENTERS).
func parseDatacenters(s string) (map[string]int, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	return config.ParseDatacenters(s)
}
