package cassandra

import (
	"context"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

func (s hazards) insertStored(ctx context.Context, h *domain.HazardRecord, bucket time.Time, hash string, previous, occurred time.Time, sequence any, traces ...map[string]string) (bool, error) {
	values := append([]any{auditShard(h.HazardID)}, storedValues(h, bucket, hash, previous, occurred)...)
	values = append(values, sequence, !domain.Withdrawn(h), h.Revision)
	b := s.db.guardBatch(ctx, canonicalGuard(h.HazardID))
	b.Query(`INSERT INTO canonical_events (shard,`+storedColumns+`,ingest_sequence,summary_active,summary_revision) VALUES (`+strings.TrimSuffix(strings.Repeat("?,", len(values)), ",")+`) IF NOT EXISTS`, values...)
	var trace map[string]string
	if len(traces) > 0 {
		trace = traces[0]
	}
	if s.db.fencedSchema {
		if err := s.db.addCriticalEvent(ctx, b, h, trace); err != nil {
			return false, err
		}
	}
	return s.db.guardedCAS(ctx, canonicalGuard(h.HazardID), b)
}

// Lazy adoption keeps old catalogs readable during background migration. The
// insert is conditional, so an older legacy row cannot replace a new revision.
func (s hazards) adoptLegacy(ctx context.Context, st *stored) error {
	// Migration 0005 owns random legacy IDs. A background scanner that observed
	// one before it was re-keyed must not recreate it after the re-key deletes it.
	if st.rec.HazardID != domain.HazardID(st.rec.Source, st.rec.SourceRefID) {
		return nil
	}
	_, err := s.insertStored(ctx, st.rec, st.bucket, st.hash, st.prevBucket, st.prevOccurredAt, st.sequence)
	return err
}
