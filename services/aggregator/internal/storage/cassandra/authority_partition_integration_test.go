//go:build integration

package cassandra

import (
	"context"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

// Verify the database primitive before moving canonical commits to guarded
// partitions. This alone is not an implementation of Cassandra write fencing.
func TestAuthorityAndMutationShareOnePaxosBoundary(t *testing.T) {
	db, err := openTest(t, "agg_test_authority_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()
	err = db.session.Query(`CREATE TABLE authority_probe (shard int, hazard_id text,
		authority_tokens map<text,text> static, authority_epochs map<text,bigint> static,
		authority_until map<text,timestamp> static, fence_guard text static,
		payload text, PRIMARY KEY (shard,hazard_id))`).WithContext(ctx).Exec()
	if err != nil {
		t.Fatal(err)
	}
	var serverNow time.Time
	if err = db.session.Query(`SELECT toTimestamp(now()) FROM system.local`).Scan(&serverNow); err != nil {
		t.Fatal(err)
	}
	install := func(epoch int64, holder string, until time.Time, previous any) bool {
		b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
		b.Query(`UPDATE authority_probe SET authority_epochs[?]=?, authority_tokens[?]=?, authority_until[?]=?
			WHERE shard=? IF authority_epochs[?]=?`, "poll", epoch, "poll", holder, "poll", until, 0, "poll", previous)
		ok, iter, e := db.session.MapExecuteBatchCAS(b, map[string]any{})
		if iter != nil {
			if closeErr := iter.Close(); e == nil {
				e = closeErr
			}
		}
		if e != nil {
			t.Fatal(e)
		}
		return ok
	}
	if !install(1, "old", serverNow.Add(time.Second), nil) {
		t.Fatal("initial authority install rejected")
	}
	write := func(id, holder string) bool {
		b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
		b.Query(`UPDATE authority_probe SET fence_guard=? WHERE shard=?
			IF authority_tokens[?]=? AND authority_until[?]>toTimestamp(now())`, holder, 0, "poll", holder, "poll")
		b.Query(`INSERT INTO authority_probe (shard,hazard_id,payload) VALUES (?,?,?) IF NOT EXISTS`, 0, id, "body")
		ok, iter, e := db.session.MapExecuteBatchCAS(b, map[string]any{})
		if iter != nil {
			if closeErr := iter.Close(); e == nil {
				e = closeErr
			}
		}
		if e != nil {
			t.Fatal(e)
		}
		return ok
	}
	if !write("before", "old") {
		t.Fatal("valid authority rejected")
	}
	time.Sleep(1100 * time.Millisecond)
	if write("unseen-after-expiry", "old") {
		t.Fatal("expired authority created a previously unseen record")
	}
	if !install(2, "new", serverNow.Add(time.Minute), int64(1)) {
		t.Fatal("new term rejected")
	}
	if install(1, "old", serverNow.Add(time.Hour), int64(1)) {
		t.Fatal("stale install overwrote a newer term")
	}
	if write("after-takeover", "old") || !write("after-takeover", "new") {
		t.Fatal("takeover did not fence old writes")
	}
	var count int
	if err = db.session.Query(`SELECT count(*) FROM authority_probe WHERE shard=?`, 0).Scan(&count); err != nil || count != 2 {
		t.Fatalf("unexpected committed records %d %v", count, err)
	}
}
