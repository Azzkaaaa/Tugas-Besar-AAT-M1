// Command storeadmin administers the Cassandra canonical store while the aggregator runs.
//
//	storeadmin bucket-period            show the period in effect and any scheduled change
//	storeadmin bucket-period set 5m     schedule a new period
//
// A new period applies to hazards whose occurred_at is at or after the first boundary that both
// the old and the new period divide, at least a minute from now. Running aggregators pick it up
// within 30 seconds; nothing needs a restart and existing buckets are not rewritten.
//
// Connection settings come from the aggregator's environment (CANONICAL_STORE_CASSANDRA_HOSTS,
// default localhost:9042, _KEYSPACE, _USERNAME, _PASSWORD, _CONSISTENCY, _LOCAL_DC). The keyspace
// must already exist with an up-to-date schema: storeadmin never creates or migrates anything,
// so a wrong keyspace name fails instead of creating an empty one.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/cassandra"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/open"
)

const usage = `usage:
  storeadmin bucket-period            show the bucket period and any scheduled change
  storeadmin bucket-period set <dur>  schedule a new bucket period (e.g. 5m, 1h)
  storeadmin prepare-list-index      backfill/cut over ordered List headers AFTER all writers are upgraded`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "storeadmin:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	prepareList := len(args) == 1 && args[0] == "prepare-list-index"
	if !prepareList && (len(args) == 0 || args[0] != "bucket-period" || (len(args) != 1 && (len(args) != 3 || args[1] != "set"))) {
		return fmt.Errorf("%s", usage)
	}
	var period time.Duration
	if len(args) == 3 {
		p, err := time.ParseDuration(args[2])
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", args[2], err)
		}
		if err := cassandra.ValidateBucketPeriod(p); err != nil {
			return err
		}
		period = p
	}

	timeout := 2 * time.Minute
	if prepareList {
		timeout = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if os.Getenv("CANONICAL_STORE_CASSANDRA_HOSTS") == "" {
		_ = os.Setenv("CANONICAL_STORE_CASSANDRA_HOSTS", "localhost:9042")
	}
	c, err := config.LoadCassandra()
	if err != nil {
		return err
	}
	cfg := open.CassandraConfig(c, 0)
	cfg.ExistingOnly = true
	db, err := cassandra.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	if prepareList {
		if err := db.PrepareOrderedList(ctx); err != nil {
			return err
		}
		fmt.Println("Ordered List index backfilled and ready; canonical data unchanged")
		return nil
	}

	if period > 0 {
		from, err := db.SetBucketPeriod(ctx, period)
		if err != nil {
			return err
		}
		if from.IsZero() {
			fmt.Printf("bucket period is already %s\n", period)
			return nil
		}
		fmt.Printf("bucket period %s scheduled from %s (hazards occurring from then on)\n", period, from.Format(time.RFC3339))
	}
	st, err := db.BucketPeriod(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("current: %s (since %s)\n", st.Current, st.CurrentFrom.Format(time.RFC3339))
	if st.Next > 0 {
		fmt.Printf("next:    %s from %s\n", st.Next, st.NextFrom.Format(time.RFC3339))
	}
	return nil
}
