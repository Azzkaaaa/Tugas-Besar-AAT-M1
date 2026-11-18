package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/jackc/pgx/v5"
)

type sources struct{ db *DB }

// legacyNoSuccess is what older builds stored as last_success_at before a stream's first
// success; it reads back as zero.
var legacyNoSuccess = time.Unix(0, 0).UTC()

func (s sources) Get(ctx context.Context, key domain.SourceKey) (storage.SourceState, error) {
	var st storage.SourceState
	var lastSuccess *time.Time
	var errMsg *string
	err := s.db.Pool.QueryRow(ctx, `
		SELECT watermark_time, last_success_at, last_attempt_at, status, error_message
		FROM ingest_watermarks WHERE source_key = $1
	`, string(key)).Scan(&st.Watermark, &lastSuccess, &st.LastAttemptAt, &st.Status, &errMsg)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.SourceState{Status: domain.StatusUnavailable}, nil
	}
	if err != nil {
		return storage.SourceState{}, err
	}
	st.Watermark = st.Watermark.UTC()
	st.LastAttemptAt = st.LastAttemptAt.UTC()
	if lastSuccess != nil && !lastSuccess.Equal(legacyNoSuccess) {
		st.LastSuccessAt = lastSuccess.UTC()
	}
	if errMsg != nil {
		st.ErrorMessage = *errMsg
	}
	return st, nil
}

func (s sources) RecordSuccess(ctx context.Context, key domain.SourceKey, watermark time.Time) error {
	return s.db.write(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
		INSERT INTO ingest_watermarks (source_key, watermark_time, last_success_at, last_attempt_at, status, error_message)
		VALUES ($1, $2, $3, $3, $4, NULL)
		ON CONFLICT (source_key) DO UPDATE SET
			watermark_time = EXCLUDED.watermark_time,
			last_success_at = EXCLUDED.last_success_at,
			last_attempt_at = EXCLUDED.last_attempt_at,
			status = EXCLUDED.status,
			error_message = NULL
	`, string(key), watermark.UTC(), time.Now().UTC(), domain.StatusFresh)
		return err
	})
}

// RecordFailure keeps the previous watermark_time and last_success_at. A first failure stores the
// zero watermark (0001-01-01, which reads back as time.Time{}), as older builds did.
func (s sources) RecordFailure(ctx context.Context, key domain.SourceKey, cause string) error {
	return s.db.write(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
		INSERT INTO ingest_watermarks (source_key, watermark_time, last_success_at, last_attempt_at, status, error_message)
		VALUES ($1, $5, NULL, $2, $3, $4)
		ON CONFLICT (source_key) DO UPDATE SET
			last_attempt_at = EXCLUDED.last_attempt_at,
			status = EXCLUDED.status,
			error_message = EXCLUDED.error_message
	`, string(key), time.Now().UTC(), domain.StatusStale, cause, time.Time{})
		return err
	})
}
