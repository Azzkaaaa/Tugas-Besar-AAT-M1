package cassandra

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/rekey"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// rekeyHazardIDs (migration 0005) moves hazards from random IDs to domain.HazardID: each
// hazards_by_id row is re-created under its derived ID (revision kept, content hash recorded), its
// List row moved and the old row deleted; then outbox rows still to publish and dead letters get
// the derived hazard_id in their column and payload. Every step is idempotent and the outbox pass
// works from the payloads themselves, so an interrupted run can simply run again. One-way.
func (db *DB) rekeyHazardIDs(ctx context.Context) error {
	// Migrations run before Open loads the bucket schedule, and rows older than
	// hazards_by_id.bucket need it to find their List row (bucket_schedule exists since 0002).
	if err := db.initSchedule(ctx, db.initialPeriod); err != nil {
		return fmt.Errorf("load bucket schedule: %w", err)
	}
	type row struct{ id, source, ref string }
	var moves []row
	iter := db.session.Query(`SELECT hazard_id, source, source_ref_id FROM hazards_by_id`).WithContext(ctx).PageSize(500).Iter()
	var r row
	for iter.Scan(&r.id, &r.source, &r.ref) {
		if domain.HazardID(r.source, r.ref) != r.id {
			moves = append(moves, r)
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	s := hazards{db}
	for _, m := range moves {
		if err := s.rekeyHazard(ctx, m.id); err != nil {
			return fmt.Errorf("re-key hazard %s: %w", m.id, err)
		}
	}
	pending, err := db.rekeyOutbox(ctx, `SELECT shard, created_at, event_id, hazard_id, payload FROM outbox_pending`, true)
	if err != nil {
		return err
	}
	dead, err := db.rekeyOutbox(ctx, `SELECT event_id, hazard_id, payload FROM outbox_dead_letters`, false)
	if err != nil {
		return err
	}
	if len(moves)+pending+dead > 0 {
		log.Printf("[Cassandra] Re-keyed %d hazards, %d pending outbox events, %d dead letters to derived IDs", len(moves), pending, dead)
	}
	return nil
}

func (s hazards) rekeyHazard(ctx context.Context, oldID string) error {
	st, err := s.readStoredTable(ctx, oldID, false, true)
	if err != nil || st == nil {
		return err
	}
	rec := *st.rec
	rec.HazardID = domain.HazardID(rec.Source, rec.SourceRefID)
	if _, err := s.db.session.Query(`INSERT INTO hazards_by_id (`+storedColumns+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
		storedValues(&rec, st.bucket, st.hash, time.Time{}, time.Time{})...).WithContext(ctx).MapScanCAS(map[string]any{}); err != nil {
		return err
	}
	b := s.db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	b.Query(`DELETE FROM hazards_by_bucket WHERE bucket = ? AND occurred_at = ? AND hazard_id = ?`,
		st.bucket, st.rec.OccurredAt, oldID)
	s.addProjection(b, &rec, st.bucket, time.Time{}, time.Time{})
	if s.db.fencedSchema {
		retired := *st.rec
		retired.Revision++
		retired.Attributes = domain.NormalizeAttributes(retired.Attributes)
		retired.Attributes["withdrawn"] = true
		addAuditProjection(b, &retired)
		b.Query(`DELETE FROM canonical_events WHERE shard=? AND hazard_id=?`, auditShard(oldID), oldID)
	}
	if err := b.Exec(); err != nil {
		return err
	}
	return s.db.session.Query(`DELETE FROM hazards_by_id WHERE hazard_id = ?`, oldID).WithContext(ctx).Exec()
}

// rekeyOutbox gives every row of query whose payload names a non-derived hazard ID the derived
// one, in its hazard_id column and payload (and, for pending rows, in outbox_events too). It
// returns how many rows it changed.
func (db *DB) rekeyOutbox(ctx context.Context, query string, pending bool) (int, error) {
	type change struct {
		shard     int
		createdAt time.Time
		eventID   string
		hazardID  string
		payload   []byte
	}
	var changes []change
	iter := db.session.Query(query).WithContext(ctx).PageSize(500).Iter()
	for {
		var c change
		var oldID string
		var payload []byte
		dest := []any{&c.eventID, &oldID, &payload}
		if pending {
			dest = append([]any{&c.shard, &c.createdAt}, dest...)
		}
		if !iter.Scan(dest...) {
			break
		}
		source, ref, payloadID, err := rekey.Identity(payload)
		if err != nil {
			_ = iter.Close()
			return 0, fmt.Errorf("outbox event %s: %w", c.eventID, err)
		}
		c.hazardID = domain.HazardID(source, ref)
		if c.hazardID == oldID && c.hazardID == payloadID {
			continue
		}
		if c.payload, err = rekey.Payload(payload, c.hazardID); err != nil {
			_ = iter.Close()
			return 0, err
		}
		changes = append(changes, c)
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}
	for _, c := range changes {
		var err error
		if pending {
			b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
			b.Query(`UPDATE outbox_pending SET hazard_id = ?, payload = ? WHERE shard = ? AND created_at = ? AND event_id = ?`,
				c.hazardID, c.payload, c.shard, c.createdAt, c.eventID)
			b.Query(`UPDATE outbox_events SET hazard_id = ?, payload = ? WHERE event_id = ?`, c.hazardID, c.payload, c.eventID)
			err = b.Exec()
		} else {
			err = db.session.Query(`UPDATE outbox_dead_letters SET hazard_id = ?, payload = ? WHERE event_id = ?`,
				c.hazardID, c.payload, c.eventID).WithContext(ctx).Exec()
		}
		if err != nil {
			return 0, fmt.Errorf("re-key outbox event %s: %w", c.eventID, err)
		}
	}
	return len(changes), nil
}
