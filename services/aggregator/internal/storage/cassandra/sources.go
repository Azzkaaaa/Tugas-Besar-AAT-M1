package cassandra

import (
	"context"
	"errors"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type sources struct{ db *DB }

func (s sources) Get(ctx context.Context, key domain.SourceKey) (storage.SourceState, error) {
	var st storage.SourceState
	var status *string
	err := s.db.session.Query(`SELECT watermark_time, last_success_at, last_attempt_at, status, error_message
		FROM source_state WHERE source_key = ?`, string(key)).WithContext(ctx).
		Scan(&st.Watermark, &st.LastSuccessAt, &st.LastAttemptAt, &status, &st.ErrorMessage)
	if errors.Is(err, gocql.ErrNotFound) {
		return storage.SourceState{Status: domain.StatusUnavailable}, nil
	}
	if err != nil {
		return storage.SourceState{}, err
	}
	st.Status = domain.StatusUnavailable
	if status != nil {
		st.Status = *status
	}
	st.Watermark = utcOrZero(st.Watermark)
	st.LastSuccessAt = utcOrZero(st.LastSuccessAt)
	st.LastAttemptAt = utcOrZero(st.LastAttemptAt)
	return st, nil
}

// RecordSuccess writes the whole row; error_message is cleared.
func (s sources) RecordSuccess(ctx context.Context, key domain.SourceKey, watermark time.Time) error {
	now := msUTC(time.Now())
	stmt := `INSERT INTO source_state
			(source_key, watermark_time, last_success_at, last_attempt_at, status, error_message)
		VALUES (?, ?, ?, ?, ?, null)`
	args := []any{string(key), nullableTime(watermark), now, now, domain.StatusFresh}
	if _, ok := leader.AuthorityFromContext(ctx); ok {
		b := s.db.guardBatch(ctx, sourceGuard(key))
		b.Query(stmt, args...)
		ok, err := s.db.guardedCAS(ctx, sourceGuard(key), b)
		if err == nil && !ok {
			return leader.ErrLostAuthority
		}
		return err
	}
	return s.db.session.Query(stmt, args...).WithContext(ctx).Exec()
}

// RecordFailure only touches the attempt columns, so watermark and last success are kept (an
// UPDATE of a missing row creates it with those columns null).
func (s sources) RecordFailure(ctx context.Context, key domain.SourceKey, cause string) error {
	stmt := `UPDATE source_state SET last_attempt_at = ?, status = ?, error_message = ? WHERE source_key = ?`
	args := []any{msUTC(time.Now()), domain.StatusStale, cause, string(key)}
	if _, ok := leader.AuthorityFromContext(ctx); ok {
		b := s.db.guardBatch(ctx, sourceGuard(key))
		b.Query(stmt, args...)
		ok, err := s.db.guardedCAS(ctx, sourceGuard(key), b)
		if err == nil && !ok {
			return leader.ErrLostAuthority
		}
		return err
	}
	return s.db.session.Query(stmt, args...).WithContext(ctx).Exec()
}

// nullableTime writes the zero time as null.
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return msUTC(t)
}

// utcOrZero maps what a null timestamp scans to (zero, or the Unix epoch in some driver
// versions) to the zero time.
func utcOrZero(t time.Time) time.Time {
	if t.IsZero() || t.Equal(time.Unix(0, 0)) {
		return time.Time{}
	}
	return t.UTC()
}
