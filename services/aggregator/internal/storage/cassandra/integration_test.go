//go:build integration

// Integration tests: compiled only with -tags integration (see services/aggregator/README.md).
// They need real databases and fail when the AGG_TEST_* variables are missing.

package cassandra

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader/leasetest"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/rekey"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/storetest"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// Tables emptied between contract cases (store_meta and schema tables are kept).
var dataTables = []string{
	"hazards_by_id", "canonical_events", "stream_inputs_by_shard", "hazards_by_bucket", "hazard_buckets", "source_state",
	"outbox_events", "outbox_pending", "outbox_dead_letters", "leader_leases",
	"audit_events_by_shard", "audit_refs_by_shard", "analytics_by_shard",
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

// The configured replication is used on creation and compared on later opens.
// Discover the fixture's real DC instead of assuming the image's default name.
func TestReplicationOnCreate(t *testing.T) {
	hosts := testHosts(t)
	ctx := context.Background()
	probe := gocql.NewCluster(hosts...)
	probe.Consistency = gocql.One
	session, err := probe.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	var dc string
	err = session.Query(`SELECT data_center FROM system.local`).Scan(&dc)
	session.Close()
	if err != nil {
		t.Fatal(err)
	}
	nts := Replication{Strategy: NetworkTopologyStrategy, Datacenters: map[string]int{dc: 1}}
	db, err := Open(ctx, Config{Hosts: hosts, Keyspace: "agg_test_nts", Consistency: "LOCAL_QUORUM",
		LocalDatacenter: dc, Replication: nts, BucketPeriod: time.Minute})
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

func TestLeases(t *testing.T) {
	db, err := openTest(t, "agg_test_leases", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	leasetest.Run(t, db.Leases(), "test-lease", 2*time.Second)
}

func revEvent(s *domain.HazardRecord) (storage.OutboxEvent, error) {
	return storage.OutboxEvent{EventID: fmt.Sprintf("pub-%s-r%d", s.HazardID, s.Revision), HazardID: s.HazardID, Source: s.Source}, nil
}

func testHazard(ref string, severity string) *domain.HazardRecord {
	return &domain.HazardRecord{Source: domain.SourceBMKG, SourceRefID: ref, HazardType: domain.HazardTypeSeismic,
		Severity: severity, AreaName: "A", OccurredAt: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC),
		Attributes: map[string]any{"magnitude": 5.0}}
}

// A revision committed on canonical_events whose projection batch never ran is
// written out by the next ApplyRevision of that content, with that revision's event ID.
func TestRepairCommittedRevision(t *testing.T) {
	db, err := openTest(t, "agg_test_repair", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()
	b := db.Backend()

	first, _, err := b.Hazards.ApplyRevision(ctx, testHazard("r", domain.SeverityNormal), revEvent)
	if err != nil {
		t.Fatal(err)
	}
	next := testHazard("r", domain.SeveritySiaga)
	next.Attributes = domain.NormalizeAttributes(next.Attributes)
	hash, _ := contentHash(next)
	bucket := db.bucketOf(next.OccurredAt)
	if applied, err := db.session.Query(`UPDATE canonical_events SET severity = ?, revision = 2, content_hash = ?,
			bucket = ?, prev_bucket = ?, prev_occurred_at = ?
		WHERE shard=? AND hazard_id = ? IF revision = 1`, domain.SeveritySiaga, hash, bucket, bucket, next.OccurredAt, auditShard(first.HazardID), first.HazardID).
		MapScanCAS(map[string]any{}); err != nil || !applied {
		t.Fatalf("simulated commit: %v, %v", applied, err)
	}

	saved, changed, err := b.Hazards.ApplyRevision(ctx, testHazard("r", domain.SeveritySiaga), revEvent)
	if err != nil || !changed || saved.Revision != 2 {
		t.Fatalf("repair: changed=%v revision=%d err=%v; want true/2", changed, saved.Revision, err)
	}
	page, _ := b.Hazards.List(ctx, storage.HazardQuery{}, storage.PageRequest{Size: 10})
	if len(page.Items) != 1 || page.Items[0].Revision != 2 || page.Items[0].Severity != domain.SeveritySiaga {
		t.Fatalf("after repair listed %+v", page.Items)
	}
	if _, changed, _ := b.Hazards.ApplyRevision(ctx, testHazard("r", domain.SeveritySiaga), revEvent); changed {
		t.Fatal("repaired revision written again")
	}
	evs, _ := b.Outbox.ClaimDue(ctx, 10, time.Minute)
	if len(evs) != 2 || evs[1].EventID != fmt.Sprintf("pub-%s-r2", first.HazardID) {
		t.Fatalf("events after repair: %+v", evs)
	}
}

func publishedPayload(t *testing.T, eventID, hazardID, ref string) []byte {
	t.Helper()
	b, err := proto.Marshal(&eventv1.HazardEventPublished{EventId: eventID, SchemaVersion: 1, Source: commonv1.Source_SOURCE_BMKG,
		Payload: &commonv1.HazardEvent{HazardId: hazardID, Source: commonv1.Source_SOURCE_BMKG, SourceRefId: ref}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Hazards stored under random IDs (before derived IDs) are re-keyed by migration 0005 with their
// revisions, List rows and pending outbox events; a second run changes nothing.
func TestRekeyMigration(t *testing.T) {
	db, err := openTest(t, "agg_test_rekey", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()
	s := hazards{db}

	// Seed a hazard the way older builds stored it: random ID, no content hash, revision 3.
	oldID := "hz-" + uuid.New().String()
	h := testHazard("legacy", domain.SeverityWaspada)
	h.HazardID, h.Revision, h.IngestedAt = oldID, 3, time.Date(2026, 3, 1, 12, 5, 0, 0, time.UTC)
	bucket := db.bucketOf(h.OccurredAt)
	if err := db.session.Query(`INSERT INTO hazards_by_id (`+hazardColumns+`, bucket) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		oldID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude,
		h.OccurredAt, h.IngestedAt, mustJSON(h.Attributes), 3, bucket).Exec(); err != nil {
		t.Fatal(err)
	}
	pb := db.session.Batch(gocql.LoggedBatch)
	s.addProjection(pb, h, bucket, time.Time{}, time.Time{})
	addOutboxEnqueue(pb, storage.OutboxEvent{EventID: "pub-old", HazardID: oldID, Source: h.Source,
		Payload: publishedPayload(t, "pub-old", oldID, "legacy")}, time.Now())
	if err := pb.Exec(); err != nil {
		t.Fatal(err)
	}
	if err := db.session.Query(`INSERT INTO outbox_dead_letters (event_id, hazard_id, source, payload, created_at, failed_at, last_error)
		VALUES ('pub-dead', ?, 'BMKG', ?, ?, ?, 'x')`, oldID, publishedPayload(t, "pub-dead", oldID, "legacy"), time.Now(), time.Now()).Exec(); err != nil {
		t.Fatal(err)
	}

	for run := 0; run < 2; run++ {
		if err := db.rekeyHazardIDs(ctx); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
	}

	newID := domain.HazardID(domain.SourceBMKG, "legacy")
	b := db.Backend()
	if old, _ := b.Hazards.Get(ctx, oldID); old != nil {
		t.Fatal("old hazard ID still resolves")
	}
	got, err := b.Hazards.Get(ctx, newID)
	if err != nil || got == nil || got.Revision != 3 || !got.IngestedAt.Equal(h.IngestedAt) || got.Severity != domain.SeverityWaspada {
		t.Fatalf("re-keyed hazard = %+v, %v", got, err)
	}
	page, _ := b.Hazards.List(ctx, storage.HazardQuery{}, storage.PageRequest{Size: 10})
	if len(page.Items) != 1 || page.Items[0].HazardID != newID {
		t.Fatalf("listed %+v", page.Items)
	}
	evs, _ := b.Outbox.ClaimDue(ctx, 10, time.Minute)
	if len(evs) != 1 || evs[0].EventID != "pub-old" || evs[0].HazardID != newID {
		t.Fatalf("pending events %+v", evs)
	}
	if _, _, payloadID, _ := rekey.Identity(evs[0].Payload); payloadID != newID {
		t.Fatalf("pending payload names %s, want %s", payloadID, newID)
	}
	var deadID string
	var deadPayload []byte
	if err := db.session.Query(`SELECT hazard_id, payload FROM outbox_dead_letters WHERE event_id = 'pub-dead'`).Scan(&deadID, &deadPayload); err != nil {
		t.Fatal(err)
	}
	if _, _, payloadID, _ := rekey.Identity(deadPayload); deadID != newID || payloadID != newID {
		t.Fatalf("dead letter hazard %s / payload %s, want %s", deadID, payloadID, newID)
	}
	// The same content again is not a revision; a change continues the revision count.
	if _, changed, err := b.Hazards.ApplyRevision(ctx, testHazard("legacy", domain.SeverityWaspada), revEvent); err != nil || changed {
		t.Fatalf("unchanged re-keyed hazard: changed=%v err=%v", changed, err)
	}
	if s4, _, err := b.Hazards.ApplyRevision(ctx, testHazard("legacy", domain.SeverityAwas), revEvent); err != nil || s4.Revision != 4 {
		t.Fatalf("revision after re-key = %+v, %v", s4, err)
	}
	if _, err := keyspaceTable(db, "hazard_by_source_ref"); err == nil {
		t.Fatal("hazard_by_source_ref still exists")
	}
}

// The re-key runs during Open, before the bucket schedule is loaded, on rows that may predate the
// bucket column: reproduce that order on a keyspace whose migration 0005 is not recorded yet.
func TestRekeyDuringOpen(t *testing.T) {
	db, err := openTest(t, "agg_test_rekey_open", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	oldID := "hz-" + uuid.New().String()
	h := testHazard("pre-bucket", domain.SeverityNormal)
	h.HazardID, h.IngestedAt = oldID, h.OccurredAt
	// A row from before the bucket and revision columns were written.
	if err := db.session.Query(`INSERT INTO hazards_by_id (`+strings.TrimSuffix(hazardColumns, ", revision")+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		oldID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude,
		h.OccurredAt, h.IngestedAt, mustJSON(h.Attributes)).Exec(); err != nil {
		t.Fatal(err)
	}
	pb := db.session.Batch(gocql.LoggedBatch)
	h.Revision = 1
	hazards{db}.addProjection(pb, h, db.bucketOf(h.OccurredAt), time.Time{}, time.Time{})
	if err := pb.Exec(); err != nil {
		t.Fatal(err)
	}
	if err := db.session.Query(`DELETE FROM schema_migrations WHERE version IN (5, 6)`).Exec(); err != nil {
		t.Fatal(err)
	}

	again, err := openTest(t, "agg_test_rekey_open", time.Minute)
	if err != nil {
		t.Fatalf("Open with pending re-key: %v", err)
	}
	defer again.Close()
	newID := domain.HazardID(domain.SourceBMKG, "pre-bucket")
	page, err := again.Backend().Hazards.List(context.Background(), storage.HazardQuery{}, storage.PageRequest{Size: 10})
	if err != nil || len(page.Items) != 1 || page.Items[0].HazardID != newID || page.Items[0].Revision != 1 {
		t.Fatalf("after re-key on Open listed %+v, %v", page.Items, err)
	}
}

// keyspaceTable reports an error when the table does not exist in db's keyspace.
func keyspaceTable(db *DB, table string) (string, error) {
	var name string
	err := db.session.Query(`SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?`,
		db.keyspace, table).Scan(&name)
	return name, err
}
