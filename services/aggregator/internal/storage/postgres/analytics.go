package postgres

import (
	"context"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func (s audit) ScanAnalytics(ctx context.Context, source string, visit func(storage.AnalyticsRow) error) ([]string, error) {
	where := `COALESCE(attributes->>'withdrawn','false') <> 'true'`
	args := []any{}
	if source != "" {
		where += ` AND source=$1`
		args = append(args, source)
	}
	rows, err := s.db.Pool.Query(ctx, `SELECT occurred_at,hazard_type,severity,area_name,
		CASE WHEN hazard_type='SEISMIC' AND jsonb_typeof(attributes->'tsunami_warnings')='array'
		THEN jsonb_array_length(attributes->'tsunami_warnings')>0 ELSE false END
		FROM hazard_events WHERE `+where, args...)
	if err != nil {
		return []string{"postgres-canonical"}, err
	}
	defer rows.Close()
	for rows.Next() {
		var row storage.AnalyticsRow
		if err = rows.Scan(&row.OccurredAt, &row.HazardType, &row.Severity, &row.Area, &row.Tsunami); err != nil {
			return []string{"postgres-canonical"}, err
		}
		if err = visit(row); err != nil {
			return []string{"postgres-canonical"}, err
		}
	}
	if err = rows.Err(); err != nil {
		return []string{"postgres-canonical"}, err
	}
	return nil, nil
}

var _ storage.AnalyticsReader = audit{}
