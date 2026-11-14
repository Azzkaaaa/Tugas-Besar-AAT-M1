//go:build integration

// Integration tests: compiled only with -tags integration (see services/aggregator/README.md).
// They need real databases and fail when the AGG_TEST_* variables are missing.

package cassandra

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/storetest"
)

// Tables emptied between contract cases (store_meta and schema tables are kept).
var dataTables = []string{
	"hazards_by_id", "hazard_by_source_ref", "hazards_by_bucket", "hazard_buckets", "source_state",
	"outbox_events", "outbox_pending", "outbox_dead_letters",
}

func testHosts(t *testing.T) []string {
	t.Helper()
	hosts := os.Getenv("AGG_TEST_CASSANDRA_HOSTS")
	if hosts == "" {
		t.Fatal("AGG_TEST_CASSANDRA_HOSTS not set")
	}
	return strings.Split(hosts, ",")
}

// openTest opens keyspace (created if missing) on AGG_TEST_CASSANDRA_HOSTS, or skips.
func openTest(t *testing.T, keyspace string, period time.Duration) (*DB, error) {
	t.Helper()
	hosts := os.Getenv("AGG_TEST_CASSANDRA_HOSTS")
	if hosts == "" {
		t.Fatal("AGG_TEST_CASSANDRA_HOSTS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	return Open(ctx, Config{
		Hosts:           strings.Split(hosts, ","),
		Keyspace:        keyspace,
		Consistency:     "ONE",
		BucketPeriod:    period,
		OutboxRetention: time.Hour,
	})
}

func dropKeyspace(db *DB) {
	_ = db.session.Query(`DROP KEYSPACE IF EXISTS ` + db.keyspace).Exec()
	db.Close()
}

// TestContract runs the storage contract with a fine and a coarse bucket period, so List is
// exercised across many buckets and within one.
func TestContract(t *testing.T) {
	for _, tc := range []struct {
		keyspace string
		period   time.Duration
	}{
		{"agg_test_contract_1m", time.Minute},
		{"agg_test_contract_24h", 24 * time.Hour},
	} {
		t.Run(tc.period.String(), func(t *testing.T) {
			db, err := openTest(t, tc.keyspace, tc.period)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { dropKeyspace(db) })
			storetest.Run(t, func(t *testing.T) *storage.Backend {
				truncate(t, db)
				return db.Backend()
			})
		})
	}
}

// TestContractAcrossScheduleChanges runs the contract with period changes in the middle of the
// test data (storetest hazards start at 2026-03-01 12:00 and span about 8h), so List, moves and
// paging cross segments.
func TestContractAcrossScheduleChanges(t *testing.T) {
	db, err := openTest(t, "agg_test_contract_sched", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()
	if err := db.addSegment(ctx, base.Add(2*time.Hour), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := db.addSegment(ctx, base.Add(5*time.Hour), 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	storetest.Run(t, func(t *testing.T) *storage.Backend {
		truncate(t, db)
		return db.Backend()
	})
}

func truncate(t *testing.T, db *DB) {
	t.Helper()
	for _, table := range dataTables {
		if err := db.session.Query(`TRUNCATE ` + table).Exec(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSetBucketPeriod(t *testing.T) {
	db, err := openTest(t, "agg_test_period", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()

	if from, err := db.SetBucketPeriod(ctx, time.Minute); err != nil || !from.IsZero() {
		t.Fatalf("setting the current period = %s, %v; want no-op", from, err)
	}
	before := time.Now()
	from, err := db.SetBucketPeriod(ctx, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if from.Before(before.Add(scheduleChangeLead)) || !aligned(from, 5*time.Minute) {
		t.Fatalf("change scheduled at %s: must be >= now+%s and on a 5m boundary", from, scheduleChangeLead)
	}
	st, err := db.BucketPeriod(ctx)
	if err != nil || st.Current != time.Minute || st.Next != 5*time.Minute || !st.NextFrom.Equal(from) {
		t.Fatalf("status = %+v, %v", st, err)
	}
	// The writer already uses the new segment for hazards from the boundary on.
	if got := db.bucketOf(from.Add(7 * time.Minute)); !got.Equal(from.Add(5 * time.Minute)) {
		t.Fatalf("bucketOf after boundary = %s", got)
	}
	if _, err := db.SetBucketPeriod(ctx, time.Hour); err == nil {
		t.Fatal("second change accepted while one is pending")
	}
	if _, err := db.SetBucketPeriod(ctx, 7*time.Minute); err == nil {
		t.Fatal("period not dividing 24h accepted")
	}

	// Reopening with another configured period keeps the stored schedule.
	other, err := openTest(t, "agg_test_period", time.Hour)
	if err != nil {
		t.Fatalf("reopening with another configured period: %v", err)
	}
	defer other.Close()
	st2, _ := other.BucketPeriod(ctx)
	if st2 != st {
		t.Fatalf("schedule changed on reopen: %+v vs %+v", st2, st)
	}
}

// A hazard written before a schedule change that covers its time (a late-known change) is still
// listed, and a revision that moves it deletes the row it was really written to.
func TestMoveAfterScheduleChange(t *testing.T) {
	db, err := openTest(t, "agg_test_move_sched", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()
	b := db.Backend()
	at := time.Date(2026, 3, 1, 22, 7, 0, 0, time.UTC)
	h := &domain.HazardRecord{Source: domain.SourceBMKG, SourceRefID: "late", HazardType: domain.HazardTypeSeismic,
		Severity: domain.SeverityNormal, AreaName: "A", OccurredAt: at, Attributes: map[string]any{}}
	noEvent := func(s *domain.HazardRecord) (storage.OutboxEvent, error) {
		return storage.OutboxEvent{EventID: "pub-" + s.HazardID + s.OccurredAt.String(), HazardID: s.HazardID, Source: s.Source}, nil
	}
	saved, _, err := b.Hazards.ApplyRevision(ctx, h, noEvent)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.addSegment(ctx, time.Date(2026, 3, 1, 22, 0, 0, 0, time.UTC), time.Hour); err != nil {
		t.Fatal(err)
	}
	page, err := b.Hazards.List(ctx, storage.HazardQuery{}, storage.PageRequest{Size: 10})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("listed %d, %v", len(page.Items), err)
	}
	rev := *h
	rev.OccurredAt = at.Add(3 * time.Hour)
	if _, changed, err := b.Hazards.ApplyRevision(ctx, &rev, noEvent); err != nil || !changed {
		t.Fatalf("move: %v, %v", changed, err)
	}
	page, err = b.Hazards.List(ctx, storage.HazardQuery{}, storage.PageRequest{Size: 10})
	if err != nil || len(page.Items) != 1 || page.Items[0].HazardID != saved.HazardID || !page.Items[0].OccurredAt.Equal(rev.OccurredAt) {
		t.Fatalf("after move listed %+v, %v", page.Items, err)
	}
	var n int
	if err := db.session.Query(`SELECT count(*) FROM hazards_by_bucket WHERE bucket = ?`, at).Scan(&n); err != nil || n != 0 {
		t.Fatalf("old List row left in its bucket: %d, %v", n, err)
	}
}

// ExistingOnly never creates a keyspace: a wrong name fails and leaves nothing behind.
func TestExistingOnly(t *testing.T) {
	hosts := testHosts(t)
	ctx := context.Background()
	_, err := Open(ctx, Config{Hosts: hosts, Keyspace: "agg_test_missing", Consistency: "ONE", ExistingOnly: true})
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("missing keyspace: %v", err)
	}

	db, err := openTest(t, "agg_test_existing", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	if repl, err := keyspaceReplication(ctx, db.session, "agg_test_missing"); err != nil || repl != nil {
		t.Fatalf("ExistingOnly created the keyspace: %v, %v", repl, err)
	}

	admin, err := Open(ctx, Config{Hosts: hosts, Keyspace: "agg_test_existing", Consistency: "ONE", ExistingOnly: true})
	if err != nil {
		t.Fatalf("existing keyspace: %v", err)
	}
	defer admin.Close()
	if st, err := admin.BucketPeriod(ctx); err != nil || st.Current != time.Minute {
		t.Fatalf("BucketPeriod via ExistingOnly = %+v, %v", st, err)
	}

	// A keyspace without the aggregator schema is refused too.
	if err := db.session.Query(`CREATE KEYSPACE IF NOT EXISTS agg_test_bare WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`).Exec(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.session.Query(`DROP KEYSPACE IF EXISTS agg_test_bare`).Exec() }()
	if _, err := Open(ctx, Config{Hosts: hosts, Keyspace: "agg_test_bare", Consistency: "ONE", ExistingOnly: true}); err == nil {
		t.Fatal("keyspace without schema accepted")
	}
}

// The configured replication is used on creation and compared on later opens. The docker image's
// datacenter is datacenter1.
func TestReplicationOnCreate(t *testing.T) {
	hosts := testHosts(t)
	ctx := context.Background()
	nts := Replication{Strategy: NetworkTopologyStrategy, Datacenters: map[string]int{"datacenter1": 1}}
	db, err := Open(ctx, Config{Hosts: hosts, Keyspace: "agg_test_nts", Consistency: "LOCAL_QUORUM",
		LocalDatacenter: "datacenter1", Replication: nts, BucketPeriod: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	actual, err := keyspaceReplication(ctx, db.session, "agg_test_nts")
	if err != nil {
		t.Fatal(err)
	}
	want, _ := nts.normalized()
	if diff := replicationMismatch(actual, want); diff != "" {
		t.Fatalf("created keyspace: %s", diff)
	}
	// Works at LOCAL_QUORUM through the DC-aware policy.
	if _, _, err := db.Backend().Hazards.ApplyRevision(ctx, &domain.HazardRecord{Source: domain.SourceBMKG, SourceRefID: "x",
		HazardType: domain.HazardTypeSeismic, Severity: domain.SeverityNormal, OccurredAt: time.Now()},
		func(s *domain.HazardRecord) (storage.OutboxEvent, error) {
			return storage.OutboxEvent{EventID: "pub-x", HazardID: s.HazardID, Source: s.Source}, nil
		}); err != nil {
		t.Fatalf("write at LOCAL_QUORUM: %v", err)
	}

	// Reopening with another replication keeps the keyspace's (and only warns).
	other, err := Open(ctx, Config{Hosts: hosts, Keyspace: "agg_test_nts", Consistency: "ONE",
		Replication: Replication{Factor: 3}, BucketPeriod: time.Minute})
	if err != nil {
		t.Fatalf("reopen with another replication: %v", err)
	}
	other.Close()
	actual, _ = keyspaceReplication(ctx, db.session, "agg_test_nts")
	if diff := replicationMismatch(actual, want); diff != "" {
		t.Fatalf("existing keyspace replication changed: %s", diff)
	}
	simple3, _ := Replication{Factor: 3}.normalized()
	if replicationMismatch(actual, simple3) == "" {
		t.Fatal("mismatch against SimpleStrategy 3 not detected")
	}
}
