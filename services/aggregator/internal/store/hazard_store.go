package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

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

// UpsertHazard inserts or updates a hazard record within an optional transaction.
// Preserves hazard_id and ingested_at across source revisions.
func (db *DB) UpsertHazard(ctx context.Context, tx pgx.Tx, h *domain.HazardRecord) (*domain.HazardRecord, bool, error) {
	attrJSON, err := json.Marshal(h.Attributes)
	if err != nil {
		attrJSON = []byte("{}")
	}

	now := time.Now().UTC()
	var executor pgx.Tx
	if tx != nil {
		executor = tx
	}

	queryExisting := `
		SELECT hazard_id, ingested_at, attributes FROM hazard_events
		WHERE source = $1 AND source_ref_id = $2
	`
	var row pgx.Row
	if executor != nil {
		row = executor.QueryRow(ctx, queryExisting, h.Source, h.SourceRefID)
	} else {
		row = db.Pool.QueryRow(ctx, queryExisting, h.Source, h.SourceRefID)
	}

	var existingID string
	var existingIngestedAt time.Time
	var existingAttrJSON []byte
	err = row.Scan(&existingID, &existingIngestedAt, &existingAttrJSON)

	if errors.Is(err, pgx.ErrNoRows) {
		// New record
		if h.HazardID == "" {
			h.HazardID = "hz-" + uuid.New().String()
		}
		if h.IngestedAt.IsZero() {
			h.IngestedAt = now
		}

		insertQuery := `
			INSERT INTO hazard_events (
				hazard_id, source, source_ref_id, hazard_type, severity,
				area_name, latitude, longitude, occurred_at, ingested_at, attributes
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`
		if executor != nil {
			_, err = executor.Exec(ctx, insertQuery,
				h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity,
				h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, attrJSON,
			)
		} else {
			_, err = db.Pool.Exec(ctx, insertQuery,
				h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity,
				h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, attrJSON,
			)
		}
		if err != nil {
			return nil, false, fmt.Errorf("insert hazard failed: %w", err)
		}
		return h, true, nil
	} else if err != nil {
		return nil, false, fmt.Errorf("check existing hazard failed: %w", err)
	}

	// Update existing record: retain hazard_id and ingested_at
	h.HazardID = existingID
	h.IngestedAt = existingIngestedAt

	updateQuery := `
		UPDATE hazard_events SET
			hazard_type = $1,
			severity = $2,
			area_name = $3,
			latitude = $4,
			longitude = $5,
			occurred_at = $6,
			attributes = $7
		WHERE hazard_id = $8
	`
	if executor != nil {
		_, err = executor.Exec(ctx, updateQuery,
			h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude,
			h.OccurredAt, attrJSON, h.HazardID,
		)
	} else {
		_, err = db.Pool.Exec(ctx, updateQuery,
			h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude,
			h.OccurredAt, attrJSON, h.HazardID,
		)
	}
	if err != nil {
		return nil, false, fmt.Errorf("update hazard failed: %w", err)
	}

	return h, true, nil
}

func (db *DB) GetHazardByID(ctx context.Context, hazardID string) (*domain.HazardRecord, error) {
	query := `
		SELECT hazard_id, source, source_ref_id, hazard_type, severity,
		       area_name, latitude, longitude, occurred_at, ingested_at, attributes
		FROM hazard_events
		WHERE hazard_id = $1
	`
	var h domain.HazardRecord
	var attrJSON []byte
	err := db.Pool.QueryRow(ctx, query, hazardID).Scan(
		&h.HazardID, &h.Source, &h.SourceRefID, &h.HazardType, &h.Severity,
		&h.AreaName, &h.Latitude, &h.Longitude, &h.OccurredAt, &h.IngestedAt, &attrJSON,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(attrJSON, &h.Attributes); err != nil {
		h.Attributes = map[string]any{}
	}
	return &h, nil
}

func (db *DB) ListHazards(ctx context.Context, filter *commonv1.HazardFilter, pageToken string, pageSize int) ([]*domain.HazardRecord, string, error) {
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}

	whereClauses := []string{"1=1"}
	args := []any{}
	argIdx := 1

	if filter != nil {
		if filter.Source == commonv1.Source_SOURCE_BMKG {
			whereClauses = append(whereClauses, fmt.Sprintf("source = $%d", argIdx))
			args = append(args, "BMKG")
			argIdx++
		} else if filter.Source == commonv1.Source_SOURCE_PVMBG {
			whereClauses = append(whereClauses, fmt.Sprintf("source = $%d", argIdx))
			args = append(args, "PVMBG")
			argIdx++
		}

		if filter.HazardType == commonv1.HazardType_HAZARD_TYPE_SEISMIC {
			whereClauses = append(whereClauses, fmt.Sprintf("hazard_type = $%d", argIdx))
			args = append(args, "SEISMIC")
			argIdx++
		} else if filter.HazardType == commonv1.HazardType_HAZARD_TYPE_VOLCANIC {
			whereClauses = append(whereClauses, fmt.Sprintf("hazard_type = $%d", argIdx))
			args = append(args, "VOLCANIC")
			argIdx++
		}

		if filter.Severity != commonv1.Severity_SEVERITY_UNSPECIFIED {
			var sevStr string
			switch filter.Severity {
			case commonv1.Severity_SEVERITY_NORMAL:
				sevStr = "NORMAL"
			case commonv1.Severity_SEVERITY_WASPADA:
				sevStr = "WASPADA"
			case commonv1.Severity_SEVERITY_SIAGA:
				sevStr = "SIAGA"
			case commonv1.Severity_SEVERITY_AWAS:
				sevStr = "AWAS"
			}
			if sevStr != "" {
				whereClauses = append(whereClauses, fmt.Sprintf("severity = $%d", argIdx))
				args = append(args, sevStr)
				argIdx++
			}
		}

		if filter.Since != nil && filter.Since.IsValid() {
			whereClauses = append(whereClauses, fmt.Sprintf("occurred_at >= $%d", argIdx))
			args = append(args, filter.Since.AsTime())
			argIdx++
		}

		if filter.Until != nil && filter.Until.IsValid() {
			whereClauses = append(whereClauses, fmt.Sprintf("occurred_at < $%d", argIdx))
			args = append(args, filter.Until.AsTime())
			argIdx++
		}
	}

	cursor, err := decodeCursor(pageToken)
	if err != nil {
		return nil, "", fmt.Errorf("invalid page token cursor: %w", err)
	}

	if cursor != nil {
		// Ordering is occurred_at DESC, hazard_id ASC
		whereClauses = append(whereClauses, fmt.Sprintf("(occurred_at < $%d OR (occurred_at = $%d AND hazard_id > $%d))", argIdx, argIdx, argIdx+1))
		args = append(args, cursor.OccurredAt, cursor.HazardID)
		argIdx += 2
	}

	// Fetch pageSize + 1 to check if there is a next page
	query := fmt.Sprintf(`
		SELECT hazard_id, source, source_ref_id, hazard_type, severity,
		       area_name, latitude, longitude, occurred_at, ingested_at, attributes
		FROM hazard_events
		WHERE %s
		ORDER BY occurred_at DESC, hazard_id ASC
		LIMIT $%d
	`, strings.Join(whereClauses, " AND "), argIdx)
	args = append(args, pageSize+1)

	rows, err := db.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("failed executing hazard list query: %w", err)
	}
	defer rows.Close()

	var records []*domain.HazardRecord
	for rows.Next() {
		var h domain.HazardRecord
		var attrJSON []byte
		if err := rows.Scan(
			&h.HazardID, &h.Source, &h.SourceRefID, &h.HazardType, &h.Severity,
			&h.AreaName, &h.Latitude, &h.Longitude, &h.OccurredAt, &h.IngestedAt, &attrJSON,
		); err != nil {
			return nil, "", err
		}
		if err := json.Unmarshal(attrJSON, &h.Attributes); err != nil {
			h.Attributes = map[string]any{}
		}
		records = append(records, &h)
	}

	var nextPageToken string
	if len(records) > pageSize {
		nextPageToken = encodeCursor(records[pageSize-1].OccurredAt, records[pageSize-1].HazardID)
		records = records[:pageSize]
	}

	return records, nextPageToken, nil
}

func (db *DB) HasCachedData(ctx context.Context, source string) (bool, error) {
	var count int
	err := db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM hazard_events WHERE source = $1", source).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (db *DB) GetWatermark(ctx context.Context, sourceKey string) (*domain.SourceStatusInfo, time.Time, error) {
	query := `
		SELECT watermark_time, last_success_at, last_attempt_at, status, error_message
		FROM ingest_watermarks
		WHERE source_key = $1
	`
	var wt, lastSuccess, lastAttempt time.Time
	var status string
	var errMsg *string
	err := db.Pool.QueryRow(ctx, query, sourceKey).Scan(&wt, &lastSuccess, &lastAttempt, &status, &errMsg)
	if errors.Is(err, pgx.ErrNoRows) {
		return &domain.SourceStatusInfo{
			Status: "UNAVAILABLE",
		}, time.Time{}, nil
	}
	if err != nil {
		return nil, time.Time{}, err
	}

	errStr := ""
	if errMsg != nil {
		errStr = *errMsg
	}

	return &domain.SourceStatusInfo{
		Status:        status,
		LastSuccessAt: lastSuccess,
		LastAttemptAt: lastAttempt,
		ErrorMessage:  errStr,
	}, wt, nil
}

func (db *DB) UpdateWatermark(ctx context.Context, sourceKey, source string, watermark time.Time, success bool, errMsg string) error {
	now := time.Now().UTC()
	var query string
	if success {
		query = `
			INSERT INTO ingest_watermarks (source_key, watermark_time, last_success_at, last_attempt_at, status, error_message)
			VALUES ($1, $2, $3, $3, 'FRESH', NULL)
			ON CONFLICT (source_key) DO UPDATE SET
				watermark_time = EXCLUDED.watermark_time,
				last_success_at = EXCLUDED.last_success_at,
				last_attempt_at = EXCLUDED.last_attempt_at,
				status = 'FRESH',
				error_message = NULL
		`
		_, err := db.Pool.Exec(ctx, query, sourceKey, watermark, now)
		return err
	}

	// Failure attempt: keep previous last_success_at and watermark_time
	query = `
		INSERT INTO ingest_watermarks (source_key, watermark_time, last_success_at, last_attempt_at, status, error_message)
		VALUES ($1, $2, '1970-01-01 00:00:00Z', $3, 'STALE', $4)
		ON CONFLICT (source_key) DO UPDATE SET
			last_attempt_at = EXCLUDED.last_attempt_at,
			status = 'STALE',
			error_message = EXCLUDED.error_message
	`
	_, err := db.Pool.Exec(ctx, query, sourceKey, watermark, now, errMsg)
	return err
}
