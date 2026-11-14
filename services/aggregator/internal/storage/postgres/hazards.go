package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/jackc/pgx/v5"
)

type hazards struct{ db *DB }

type PageCursor struct {
	OccurredAt time.Time `json:"o"`
	HazardID   string    `json:"h"`
}

func encodeCursor(t time.Time, id string) string {
	b, _ := json.Marshal(PageCursor{OccurredAt: t, HazardID: id})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (*PageCursor, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var c PageCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

const hazardColumns = `hazard_id, source, source_ref_id, hazard_type, severity,
	area_name, latitude, longitude, occurred_at, ingested_at, attributes, revision`

func scanHazard(row pgx.Row) (*domain.HazardRecord, error) {
	var h domain.HazardRecord
	var attrJSON []byte
	if err := row.Scan(
		&h.HazardID, &h.Source, &h.SourceRefID, &h.HazardType, &h.Severity,
		&h.AreaName, &h.Latitude, &h.Longitude, &h.OccurredAt, &h.IngestedAt, &attrJSON, &h.Revision,
	); err != nil {
		return nil, err
	}
	h.OccurredAt = h.OccurredAt.UTC()
	h.IngestedAt = h.IngestedAt.UTC()
	if err := json.Unmarshal(attrJSON, &h.Attributes); err != nil || h.Attributes == nil {
		h.Attributes = map[string]any{}
	}
	return &h, nil
}

// msUTC is the precision every backend stores times at: UTC, milliseconds.
func msUTC(t time.Time) time.Time {
	return t.UTC().Truncate(time.Millisecond)
}

// ApplyRevision runs the lookup, the write and the outbox insert in one transaction.
func (s hazards) ApplyRevision(ctx context.Context, rev *domain.HazardRecord,
	newEvent func(saved *domain.HazardRecord) (storage.OutboxEvent, error),
) (*domain.HazardRecord, bool, error) {
	tx, err := s.db.Pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)

	saved, changed, err := applyRevision(ctx, tx, rev)
	if err != nil || !changed {
		return saved, false, err
	}
	ev, err := newEvent(saved)
	if err != nil {
		return nil, false, fmt.Errorf("build outbox event: %w", err)
	}
	if err := enqueueOutbox(ctx, tx, ev); err != nil {
		return nil, false, fmt.Errorf("enqueue outbox event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return saved, true, nil
}

// applyRevision locks the record's row, so concurrent writers of one record (two leaders during
// an overlap) run one after the other: the second sees the first's content and writes nothing.
func applyRevision(ctx context.Context, tx pgx.Tx, rev *domain.HazardRecord) (*domain.HazardRecord, bool, error) {
	h := *rev
	h.Attributes = domain.NormalizeAttributes(rev.Attributes)
	h.OccurredAt = msUTC(rev.OccurredAt)
	attrJSON, err := json.Marshal(h.Attributes)
	if err != nil {
		return nil, false, err
	}
	h.HazardID = domain.HazardID(h.Source, h.SourceRefID)
	lockExisting := func() (*domain.HazardRecord, error) {
		return scanHazard(tx.QueryRow(ctx, `SELECT `+hazardColumns+`
			FROM hazard_events WHERE hazard_id = $1 FOR UPDATE`, h.HazardID))
	}

	existing, err := lockExisting()
	if errors.Is(err, pgx.ErrNoRows) {
		if h.IngestedAt.IsZero() {
			h.IngestedAt = time.Now()
		}
		h.IngestedAt = msUTC(h.IngestedAt)
		h.Revision = 1
		tag, insertErr := tx.Exec(ctx, `INSERT INTO hazard_events (`+hazardColumns+`)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT DO NOTHING`, // hazard_id and uq_source_ref conflict together (one is derived from the other)
			h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity,
			h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, attrJSON, h.Revision,
		)
		if insertErr != nil {
			return nil, false, fmt.Errorf("insert hazard failed: %w", insertErr)
		}
		if tag.RowsAffected() == 1 {
			return &h, true, nil
		}
		// A concurrent writer inserted it first: continue as an update of its row.
		existing, err = lockExisting()
	}
	if err != nil {
		return nil, false, fmt.Errorf("check existing hazard failed: %w", err)
	}

	// Existing record: keep ingested_at.
	h.IngestedAt = existing.IngestedAt
	h.Revision = existing.Revision
	if sameContent(existing, &h) {
		return &h, false, nil
	}
	h.Revision++
	_, err = tx.Exec(ctx, `UPDATE hazard_events SET
			hazard_type = $1, severity = $2, area_name = $3, latitude = $4, longitude = $5,
			occurred_at = $6, attributes = $7, revision = $8
		WHERE hazard_id = $9`,
		h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, attrJSON, h.Revision, h.HazardID,
	)
	if err != nil {
		return nil, false, fmt.Errorf("update hazard failed: %w", err)
	}
	return &h, true, nil
}

// sameContent compares the fields a source revision can change. Both records must have
// normalized attributes.
func sameContent(a, b *domain.HazardRecord) bool {
	return a.HazardType == b.HazardType &&
		a.Severity == b.Severity &&
		a.AreaName == b.AreaName &&
		a.Latitude == b.Latitude &&
		a.Longitude == b.Longitude &&
		a.OccurredAt.Equal(b.OccurredAt) &&
		reflect.DeepEqual(a.Attributes, b.Attributes)
}

func (s hazards) Get(ctx context.Context, hazardID string) (*domain.HazardRecord, error) {
	h, err := scanHazard(s.db.Pool.QueryRow(ctx, `SELECT `+hazardColumns+` FROM hazard_events WHERE hazard_id = $1`, hazardID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return h, err
}

func (s hazards) List(ctx context.Context, q storage.HazardQuery, page storage.PageRequest) (storage.HazardPage, error) {
	where := []string{"1=1"}
	args := []any{}
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if q.Source != "" {
		where = append(where, "source = "+arg(q.Source))
	}
	if q.HazardType != "" {
		where = append(where, "hazard_type = "+arg(q.HazardType))
	}
	if q.Severity != "" {
		where = append(where, "severity = "+arg(q.Severity))
	}
	if !q.Since.IsZero() {
		where = append(where, "occurred_at >= "+arg(q.Since))
	}
	if !q.Until.IsZero() {
		where = append(where, "occurred_at < "+arg(q.Until))
	}

	cursor, err := decodeCursor(page.Token)
	if err != nil {
		return storage.HazardPage{}, fmt.Errorf("%w: %v", storage.ErrInvalidPageToken, err)
	}
	if cursor != nil {
		// Ordering is occurred_at DESC, hazard_id ASC.
		o, id := arg(cursor.OccurredAt), arg(cursor.HazardID)
		where = append(where, fmt.Sprintf("(occurred_at < %s OR (occurred_at = %s AND hazard_id > %s))", o, o, id))
	}

	// One extra row tells whether there is a next page.
	query := `SELECT ` + hazardColumns + ` FROM hazard_events WHERE ` + strings.Join(where, " AND ") +
		` ORDER BY occurred_at DESC, hazard_id ASC LIMIT ` + arg(page.Size+1)
	rows, err := s.db.Pool.Query(ctx, query, args...)
	if err != nil {
		return storage.HazardPage{}, fmt.Errorf("failed executing hazard list query: %w", err)
	}
	defer rows.Close()

	var out storage.HazardPage
	for rows.Next() {
		h, err := scanHazard(rows)
		if err != nil {
			return storage.HazardPage{}, err
		}
		out.Items = append(out.Items, h)
	}
	if err := rows.Err(); err != nil {
		return storage.HazardPage{}, err
	}
	if len(out.Items) > page.Size {
		last := out.Items[page.Size-1]
		out.NextToken = encodeCursor(last.OccurredAt, last.HazardID)
		out.Items = out.Items[:page.Size]
	}
	return out, nil
}

func (s hazards) HasData(ctx context.Context, source string) (bool, error) {
	var exists bool
	err := s.db.Pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM hazard_events WHERE source = $1)", source).Scan(&exists)
	return exists, err
}
