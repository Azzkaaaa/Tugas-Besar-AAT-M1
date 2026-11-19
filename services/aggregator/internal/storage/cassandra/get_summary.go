package cassandra

import (
	"context"
	"errors"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// One body at a time for all cold Get and List readers on this connection pool.
// Waiters hold only headers and respect their caller's deadline.
func (db *DB) acquireSummaryFallback(ctx context.Context) (func(), error) {
	db.summaryOnce.Do(func() { db.summarySlot = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case db.summarySlot <- struct{}{}:
		return func() { <-db.summarySlot }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s hazards) GetSummary(ctx context.Context, id string) (*domain.HazardRecord, error) {
	if criticalInternalKey(id) {
		return nil, nil
	}
	if s.db.fencedSchema {
		h, found, err := s.getSummaryTable(ctx, id, false)
		if found || err != nil {
			return h, err
		}
	}
	h, _, err := s.getSummaryTable(ctx, id, true)
	return h, err
}

// found distinguishes an authoritative withdrawn row from an absent row: an
// active legacy row must not resurrect a withdrawn canonical revision.
func (s hazards) getSummaryTable(ctx context.Context, id string, legacy bool) (*domain.HazardRecord, bool, error) {
	table, where := "canonical_events", "shard=? AND hazard_id=?"
	args := []any{auditShard(id), id}
	if legacy {
		table, where, args = "hazards_by_id", "hazard_id=?", []any{id}
	}
	var h domain.HazardRecord
	var revision, cachedRevision *int
	var active *bool
	err := s.db.session.Query(`SELECT hazard_id,source,hazard_type,severity,area_name,occurred_at,ingested_at,revision,
		summary_active,summary_revision FROM `+table+` WHERE `+where, args...).WithContext(ctx).
		Scan(&h.HazardID, &h.Source, &h.HazardType, &h.Severity, &h.AreaName, &h.OccurredAt, &h.IngestedAt,
			&revision, &active, &cachedRevision)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if revision != nil && cachedRevision != nil && *revision == *cachedRevision && active != nil {
		if !*active {
			return nil, true, nil
		}
		h.Revision = max(1, *revision)
		h.OccurredAt, h.IngestedAt = h.OccurredAt.UTC(), h.IngestedAt.UTC()
		return &h, true, nil
	}
	release, err := s.db.acquireSummaryFallback(ctx)
	if err != nil {
		return nil, true, err
	}
	defer release()
	// Re-read current content after admission; the row may have changed while
	// waiting. Capture the raw nullable revision for the cache's CAS condition.
	full, err := scanHazard(func(dest ...any) error {
		dest[len(dest)-1] = &revision
		return s.db.session.Query(`SELECT `+hazardColumns+` FROM `+table+` WHERE `+where, args...).WithContext(ctx).Scan(dest...)
	})
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if revision != nil {
		full.Revision = max(1, *revision)
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		cacheArgs := append([]any{!domain.Withdrawn(full), *revision}, args...)
		cacheArgs = append(cacheArgs, *revision)
		_, _ = s.db.session.Query(`UPDATE `+table+` SET summary_active=?,summary_revision=? WHERE `+where+` IF revision=?`,
			cacheArgs...).WithContext(cacheCtx).MapScanCAS(map[string]any{})
	}
	if domain.Withdrawn(full) {
		return nil, true, nil
	}
	return storage.HazardSummaryHeader(full), true, nil
}
