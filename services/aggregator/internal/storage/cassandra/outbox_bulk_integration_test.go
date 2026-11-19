//go:build integration

package cassandra

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

// Opt in with a 512MiB application client and a 1GiB Cassandra fixture. The
// pending partition exceeds one client's maximum CQL response frame if read
// with payloads in bulk. Metadata scanning must never fetch that frame.
func TestOutboxLargeBacklogBudget(t *testing.T) {
	if os.Getenv("AGG_TEST_OUTBOX_STRESS") != "1" {
		t.Skip("set AGG_TEST_OUTBOX_STRESS=1 in a memory-capped disposable process")
	}
	db, err := openTest(t, "agg_test_outbox_bulk_"+uuid.NewString()[:8], time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer dropKeyspace(db)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	started := time.Now()
	record := largeSatelliteRecord(t)
	record.HazardID = domain.HazardID(record.Source, record.SourceRefID)
	record.Attributes = domain.NormalizeAttributes(record.Attributes)
	const backlog = 32
	var lastBody []byte
	expected := make(map[string][32]byte, backlog)
	var firstID string
	setupRetries := 0
	// Test oldest-first order without depending on host wall-clock corrections.
	createdBase := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	for i := 0; i < backlog; i++ {
		record.Revision = i + 1
		record.Attributes["stream_sequence"] = strconv.Itoa(i + 1)
		record.Attributes["total_frp_mw"] = float64(i+1) * 100
		ev, err := largeSatelliteEvent(record)
		if err != nil {
			t.Fatal(err)
		}
		expected[ev.EventID] = sha256.Sum256(ev.Payload)
		if firstID == "" {
			firstID = ev.EventID
		}
		// A timeout may still have committed one copy. Retry the exact same
		// event/time keys so fixture setup remains idempotent under backpressure.
		created := createdBase.Add(time.Duration(i) * time.Millisecond)
		for attempt := 0; ; attempt++ {
			batch := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
			addOutboxEnqueue(batch, ev, created)
			err = db.execRecoverableBatch(ctx, batch)
			if err == nil {
				break
			}
			var writeTimeout *gocql.RequestErrWriteTimeout
			transient := errors.Is(err, gocql.ErrTimeoutNoResponse) || errors.As(err, &writeTimeout)
			if attempt >= 5 || !transient || ctx.Err() != nil {
				t.Fatal("enqueue large backlog", i, err)
			}
			setupRetries++
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Duration(attempt+1) * 250 * time.Millisecond):
			}
		}
		lastBody = ev.Payload
	}
	if len(lastBody)*backlog <= 256<<20 {
		t.Fatal("fixture must exceed the old bulk-response frame budget")
	}
	b := db.Backend()
	first, err := b.Outbox.ClaimDue(ctx, 50, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatal("large backlog was hydrated in bulk", len(first), err)
	}
	if len(first[0].Payload) < 8<<20 {
		t.Fatal("large head was truncated")
	}
	if first[0].EventID != firstID || sha256.Sum256(first[0].Payload) != expected[firstID] {
		t.Fatalf("oldest complete payload was not preserved: got_id=%s want_id=%s got_sha=%x want_sha=%x created_at=%s", first[0].EventID, firstID, sha256.Sum256(first[0].Payload), expected[firstID], first[0].CreatedAt)
	}
	second, err := b.Outbox.ClaimDue(ctx, 50, time.Minute)
	if err != nil || len(second) != 1 || second[0].EventID == first[0].EventID {
		t.Fatal("unreturned backlog was leased or lost", len(second), err)
	}
	if bytes.Equal(second[0].Payload, first[0].Payload) {
		t.Fatal("distinct revisions were conflated")
	}
	if digest, ok := expected[second[0].EventID]; !ok || sha256.Sum256(second[0].Payload) != digest {
		t.Fatal("second complete payload was not preserved")
	}
	if err := b.Outbox.Release(ctx, []string{first[0].EventID}); err != nil {
		t.Fatal(err)
	}
	again, err := b.Outbox.ClaimDue(ctx, 50, time.Minute)
	if err != nil || len(again) != 1 || again[0].EventID != first[0].EventID {
		t.Fatal("release did not preserve oldest-first delivery", len(again), err)
	}
	var rss, peak string
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				rss = strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
			}
		}
	}
	if value, err := os.ReadFile("/sys/fs/cgroup/memory.peak"); err == nil {
		peak = strings.TrimSpace(string(value))
	}
	t.Logf("pending=%d event_bytes=%d pending_bytes=%d batch_events=1 setup_retries=%d peak_rss=%s container_peak_bytes=%s elapsed=%s", backlog, len(lastBody), backlog*len(lastBody), setupRetries, rss, peak, time.Since(started))
}
