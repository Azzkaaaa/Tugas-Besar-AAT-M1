package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/jackc/pgx/v5"
)

type criticalJournal struct{ db *DB }

func enqueueCritical(ctx context.Context, tx pgx.Tx, h *domain.HazardRecord, trace map[string]string) error {
	e, err := storage.NewCriticalEvent(h, trace)
	if err != nil || e == nil {
		return err
	}
	encodedTrace, err := json.Marshal(e.TraceContext)
	if err != nil {
		return err
	}
	var generation int64
	if err = tx.QueryRow(ctx, `UPDATE critical_journal_clock SET version=version+1 WHERE id=1 RETURNING version`).Scan(&generation); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO critical_event_journal (event_id,hazard_id,source_ref_id,payload,payload_sha256,trace_context,generation)
		VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, e.EventID, e.HazardID, e.SourceRefID, []byte(e.Payload), e.PayloadSHA256, encodedTrace, generation)
	if err == nil && tag.RowsAffected() == 0 {
		_, err = tx.Exec(ctx, `UPDATE critical_journal_clock SET version=version-1 WHERE id=1`)
	}
	return err
}

const criticalColumns = `event_id,hazard_id,source_ref_id,payload,payload_sha256,trace_context,created_at,receipt,generation`

func scanCritical(row pgx.Row) (storage.CriticalEvent, error) {
	var e storage.CriticalEvent
	var encodedTrace, receipt []byte
	err := row.Scan(&e.EventID, &e.HazardID, &e.SourceRefID, &e.Payload, &e.PayloadSHA256, &encodedTrace, &e.CreatedAt, &receipt, &e.Generation)
	if err != nil {
		return e, err
	}
	if err := json.Unmarshal(encodedTrace, &e.TraceContext); err != nil {
		return e, err
	}
	if len(receipt) > 0 {
		if err := json.Unmarshal(receipt, &e.Receipt); err != nil {
			return e, err
		}
	}
	e.CreatedAt = e.CreatedAt.UTC()
	return e, nil
}
func (s criticalJournal) Pending(ctx context.Context, cursor storage.CriticalCursor, limit int) (storage.CriticalPage, error) {
	return s.page(ctx, cursor, limit, true, nil)
}
func (s criticalJournal) List(ctx context.Context, cursor storage.CriticalCursor, limit int) (storage.CriticalPage, error) {
	return s.page(ctx, cursor, limit, false, nil)
}
func (s criticalJournal) ListSnapshot(ctx context.Context, cursor storage.CriticalCursor, limit int, versions []int64) (storage.CriticalPage, error) {
	if len(versions) != 1 {
		return storage.CriticalPage{}, storage.ErrInvalidPageToken
	}
	return s.page(ctx, cursor, limit, false, versions)
}
func (s criticalJournal) Version(ctx context.Context) ([]int64, error) {
	var version int64
	err := s.db.Pool.QueryRow(ctx, `SELECT version FROM critical_journal_clock WHERE id=1`).Scan(&version)
	return []int64{version}, err
}
func (s criticalJournal) page(ctx context.Context, cursor storage.CriticalCursor, limit int, pending bool, versions []int64) (storage.CriticalPage, error) {
	page := storage.CriticalPage{Events: []storage.CriticalEvent{}}
	if cursor.Shard != 0 || limit < 1 || limit > 100 {
		return page, storage.ErrInvalidPageToken
	}
	query := `SELECT ` + criticalColumns + ` FROM critical_event_journal WHERE event_id>$1`
	if pending {
		query += ` AND receipt IS NULL`
	}
	arguments := []any{cursor.After, limit + 1}
	if versions != nil {
		query += ` AND generation<=$3`
		arguments = append(arguments, versions[0])
	}
	query += ` ORDER BY event_id LIMIT $2`
	rows, err := s.db.Pool.Query(ctx, query, arguments...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	payloadBytes := 0
	for rows.Next() {
		if len(page.Events) == limit {
			last := page.Events[len(page.Events)-1]
			page.Next = &storage.CriticalCursor{After: last.EventID}
			break
		}
		e, err := scanCritical(rows)
		if err != nil {
			return page, err
		}
		if len(page.Events) > 0 && payloadBytes+storage.CriticalEventBytes(e) > 4<<20 {
			last := page.Events[len(page.Events)-1]
			page.Next = &storage.CriticalCursor{After: last.EventID}
			break
		}
		page.Events = append(page.Events, e)
		payloadBytes += storage.CriticalEventBytes(e)
	}
	return page, rows.Err()
}
func (s criticalJournal) Get(ctx context.Context, hazardID, eventID string) (*storage.CriticalEvent, error) {
	e, err := scanCritical(s.db.Pool.QueryRow(ctx, `SELECT `+criticalColumns+` FROM critical_event_journal WHERE event_id=$1 AND hazard_id=$2`, eventID, hazardID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrCriticalNotFound
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}
func (s criticalJournal) RecordReceipt(ctx context.Context, receipt storage.CriticalReceipt) error {
	receipt.RecordedAt = time.Time{}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	tag, err := s.db.Pool.Exec(ctx, `UPDATE critical_event_journal SET received_at=clock_timestamp(),
		receipt=$1::jsonb || jsonb_build_object('recorded_at',clock_timestamp()) WHERE event_id=$2 AND hazard_id=$3 AND payload_sha256=$4 AND receipt IS NULL`, encoded, receipt.EventID, receipt.HazardID, receipt.PayloadSHA256)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	e, err := s.Get(ctx, receipt.HazardID, receipt.EventID)
	if err != nil {
		return err
	}
	if e.PayloadSHA256 == receipt.PayloadSHA256 && e.Receipt != nil && e.Receipt.ReceiverID == receipt.ReceiverID {
		return nil
	}
	return storage.ErrCriticalReceiptConflict
}

var _ storage.CriticalJournal = criticalJournal{}
