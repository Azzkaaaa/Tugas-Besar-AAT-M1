package cassandra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// The elected publisher serves separate coastal and regular partitions.
func pendingPartition(source string) int {
	if source == domain.SourceCoastal {
		return 1
	}
	return 0
}

// claimScanLimit bounds the pending rows one claim reads. Due events behind that many events
// still in backoff wait until those are due (at most the max backoff).
const claimScanLimit = 1000

// outbox is the single-consumer outbox. Events live in outbox_events (by id) and, until they are
// published or dead-lettered, in outbox_pending (by creation time). The lease of a claim is
// next_attempt_at, as in Postgres, but claims are plain writes: two concurrent claimers could
// take the same event, which is why Backend.OutboxSingleConsumer is true.
type outbox struct {
	db *DB
	mu sync.Mutex
	// scanFrom is the creation time of the oldest event pending at the last claim. Claims start
	// there, so they skip the tombstones of the events published before it.
	scanFrom [2]time.Time
}

// addOutboxEnqueue adds the writes that enqueue ev to b (see hazards.ApplyRevision).
func addOutboxEnqueue(b *gocql.Batch, ev storage.OutboxEvent, now time.Time) {
	createdAt := msUTC(now)
	traceContext := encodeTraceContext(ev.TraceContext)
	b.Query(`INSERT INTO outbox_events (event_id, hazard_id, source, payload, trace_context, created_at, retry_count)
		VALUES (?, ?, ?, ?, ?, ?, 0)`, ev.EventID, ev.HazardID, ev.Source, ev.Payload, traceContext, createdAt)
	b.Query(`INSERT INTO outbox_pending (shard, created_at, event_id, hazard_id, source, payload, trace_context,
			retry_count, next_attempt_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)`,
		pendingPartition(ev.Source), createdAt, ev.EventID, ev.HazardID, ev.Source, ev.Payload, traceContext, createdAt)
}

// encodeTraceContext returns the carrier as JSON, or nil (null) when there is no trace.
func encodeTraceContext(carrier map[string]string) any {
	if len(carrier) == 0 {
		return nil
	}
	b, err := json.Marshal(carrier)
	if err != nil {
		return nil
	}
	return string(b)
}

// decodeTraceContext ignores malformed values: a lost trace link must never block publishing.
func decodeTraceContext(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	var carrier map[string]string
	if err := json.Unmarshal([]byte(raw), &carrier); err != nil {
		return nil
	}
	return carrier
}

func (o *outbox) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]*storage.ClaimedEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	now := time.Now().UTC()
	var claimed []*storage.ClaimedEvent
	// Separate coastal partition prevents satellite backlog from hiding warnings.
	// Reserve half each batch for regular sources to avoid starvation in either direction.
	for _, shard := range []int{1, 0} {
		allowance := limit - len(claimed)
		if shard == 1 {
			allowance = max(1, limit/2)
		}
		iter := o.db.session.Query(`SELECT created_at, event_id, hazard_id, source, payload, trace_context, retry_count, next_attempt_at
            FROM outbox_pending WHERE shard = ? AND created_at >= ? LIMIT `+strconv.Itoa(claimScanLimit),
			shard, o.scanFrom[shard]).WithContext(ctx).PageSize(200).Iter()
		var oldest time.Time
		var rec storage.ClaimedEvent
		var traceContext string
		var nextAttempt time.Time
		taken := 0
		for iter.Scan(&rec.CreatedAt, &rec.EventID, &rec.HazardID, &rec.Source, &rec.Payload, &traceContext, &rec.RetryCount, &nextAttempt) {
			if oldest.IsZero() {
				oldest = rec.CreatedAt
			}
			if taken < allowance && !nextAttempt.After(now) {
				ev := rec
				ev.CreatedAt = ev.CreatedAt.UTC()
				ev.Payload = append([]byte(nil), rec.Payload...)
				ev.TraceContext = decodeTraceContext(traceContext)
				claimed = append(claimed, &ev)
				taken++
			}
			rec, traceContext, nextAttempt = storage.ClaimedEvent{}, "", time.Time{}
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
		if !oldest.IsZero() {
			o.scanFrom[shard] = oldest
		}
	}

	claimed, err := o.dropFinished(ctx, claimed)
	if err != nil {
		return nil, err
	}

	leaseUntil := msUTC(now.Add(lease))
	for _, ev := range claimed {
		if err := o.db.session.Query(`UPDATE outbox_pending SET next_attempt_at = ?
			WHERE shard = ? AND created_at = ? AND event_id = ?`,
			leaseUntil, pendingPartition(ev.Source), ev.CreatedAt, ev.EventID).WithContext(ctx).Exec(); err != nil {
			return nil, err
		}
	}
	return claimed, nil
}

// dropFinished removes pending rows whose event is already published or dead-lettered and returns
// the rest. Such a row is the second copy of an event enqueued twice (the same revision written by
// two writers, which gives the same event ID); publishing it again would only make consumers
// deduplicate it.
func (o *outbox) dropFinished(ctx context.Context, evs []*storage.ClaimedEvent) ([]*storage.ClaimedEvent, error) {
	out := evs[:0]
	for _, ev := range evs {
		var publishedAt, failedAt time.Time
		err := o.db.session.Query(`SELECT published_at, failed_at FROM outbox_events WHERE event_id = ?`, ev.EventID).
			WithContext(ctx).Scan(&publishedAt, &failedAt)
		if err != nil && !errors.Is(err, gocql.ErrNotFound) {
			return nil, err
		}
		if utcOrZero(publishedAt).IsZero() && utcOrZero(failedAt).IsZero() {
			out = append(out, ev)
			continue
		}
		if err := o.db.session.Query(`DELETE FROM outbox_pending WHERE shard = ? AND created_at = ? AND event_id = ?`,
			pendingPartition(ev.Source), ev.CreatedAt, ev.EventID).WithContext(ctx).Exec(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// lookup returns the outbox_events row of eventID.
func (o *outbox) lookup(ctx context.Context, eventID string) (*storage.ClaimedEvent, error) {
	var ev storage.ClaimedEvent
	var traceContext string
	err := o.db.session.Query(`SELECT event_id, hazard_id, source, payload, trace_context, created_at, retry_count
		FROM outbox_events WHERE event_id = ?`, eventID).WithContext(ctx).
		Scan(&ev.EventID, &ev.HazardID, &ev.Source, &ev.Payload, &traceContext, &ev.CreatedAt, &ev.RetryCount)
	if err != nil {
		return nil, err
	}
	ev.CreatedAt = ev.CreatedAt.UTC()
	ev.TraceContext = decodeTraceContext(traceContext)
	return &ev, nil
}

func (o *outbox) ttl() string {
	secs := int64(o.db.retention / time.Second)
	if secs <= 0 {
		secs = int64(7 * 24 * time.Hour / time.Second)
	}
	return strconv.FormatInt(secs, 10)
}

// finish removes ev from outbox_pending and rewrites its outbox_events row with the retention
// TTL, plus any extra writes, in one logged batch.
func (o *outbox) finish(ctx context.Context, ev *storage.ClaimedEvent, publishedAt, failedAt any, lastErr any,
	extra func(b *gocql.Batch),
) error {
	b := o.db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	b.Query(`DELETE FROM outbox_pending WHERE shard = ? AND created_at = ? AND event_id = ?`,
		pendingPartition(ev.Source), ev.CreatedAt, ev.EventID)
	b.Query(`INSERT INTO outbox_events (event_id, hazard_id, source, payload, trace_context, created_at, retry_count,
			last_error, published_at, failed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL `+o.ttl(),
		ev.EventID, ev.HazardID, ev.Source, ev.Payload, encodeTraceContext(ev.TraceContext), ev.CreatedAt, ev.RetryCount,
		lastErr, publishedAt, failedAt)
	if extra != nil {
		extra(b)
	}
	return b.Exec()
}

func (o *outbox) Ack(ctx context.Context, eventID string) error {
	ev, err := o.lookup(ctx, eventID)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return o.finish(ctx, ev, msUTC(time.Now()), nil, nil, nil)
}

func (o *outbox) DeadLetter(ctx context.Context, ev *storage.ClaimedEvent, reason string) error {
	if ev.CreatedAt.IsZero() {
		stored, err := o.lookup(ctx, ev.EventID)
		if err != nil {
			return err
		}
		ev = stored
	}
	now := msUTC(time.Now())
	return o.finish(ctx, ev, nil, now, reason, func(b *gocql.Batch) {
		b.Query(`INSERT INTO outbox_dead_letters (event_id, hazard_id, source, payload, trace_context, created_at,
				failed_at, last_error)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			ev.EventID, ev.HazardID, ev.Source, ev.Payload, encodeTraceContext(ev.TraceContext), ev.CreatedAt, now, reason)
	})
}

// RetryLater and Release first check that the pending row still exists, so they never recreate
// one already published (an UPDATE of a missing row would insert a partial row).
func (o *outbox) RetryLater(ctx context.Context, eventID, lastErr string, backoff time.Duration) error {
	ev, err := o.lookup(ctx, eventID)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var retryCount int
	err = o.db.session.Query(`SELECT retry_count FROM outbox_pending WHERE shard = ? AND created_at = ? AND event_id = ?`,
		pendingPartition(ev.Source), ev.CreatedAt, eventID).WithContext(ctx).Scan(&retryCount)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	retryCount++
	b := o.db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	b.Query(`UPDATE outbox_pending SET retry_count = ?, last_error = ?, next_attempt_at = ?
		WHERE shard = ? AND created_at = ? AND event_id = ?`,
		retryCount, lastErr, msUTC(time.Now().Add(backoff)), pendingPartition(ev.Source), ev.CreatedAt, eventID)
	b.Query(`UPDATE outbox_events SET retry_count = ?, last_error = ? WHERE event_id = ?`, retryCount, lastErr, eventID)
	return b.Exec()
}

func (o *outbox) Release(ctx context.Context, eventIDs []string) error {
	now := msUTC(time.Now())
	for _, id := range eventIDs {
		ev, err := o.lookup(ctx, id)
		if errors.Is(err, gocql.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if _, err := o.db.session.Query(`UPDATE outbox_pending SET next_attempt_at = ?
			WHERE shard = ? AND created_at = ? AND event_id = ? IF EXISTS`,
			now, pendingPartition(ev.Source), ev.CreatedAt, id).WithContext(ctx).MapScanCAS(map[string]any{}); err != nil {
			return fmt.Errorf("release outbox event %s: %w", id, err)
		}
	}
	return nil
}

// Prune is a no-op: finished events expire through their TTL.
func (o *outbox) Prune(context.Context, time.Duration, int) (int64, error) {
	return 0, nil
}
