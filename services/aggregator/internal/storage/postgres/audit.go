package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type audit struct{ db *DB }

func (s audit) Events(ctx context.Context, q storage.AuditQuery) (storage.AuditPage, error) {
	out := storage.AuditPage{}
	where := []string{"COALESCE(attributes->>'withdrawn', 'false') <> 'true'"}
	args := []any{}
	arg := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }
	if q.Source != "" {
		where = append(where, "source = "+arg(q.Source))
	}
	if !q.Since.IsZero() {
		where = append(where, "ingested_at >= "+arg(q.Since))
	}
	if q.After.ID != "" {
		where = append(where, "(ingested_at, hazard_id) > ("+arg(q.After.At)+", "+arg(q.After.ID)+")")
	}
	rows, err := s.db.Pool.Query(ctx, `SELECT `+hazardColumns+` FROM hazard_events WHERE `+strings.Join(where, " AND ")+` ORDER BY ingested_at, hazard_id LIMIT `+arg(q.Limit+1), args...)
	if err != nil {
		out.UnavailableUnits = []string{"postgres-canonical"}
		return out, err
	}
	defer rows.Close()
	bytes := 0
	for rows.Next() {
		h, err := scanHazard(rows)
		if err != nil {
			out.UnavailableUnits = []string{"postgres-canonical"}
			return out, err
		}
		encoded, _ := json.Marshal(h)
		if len(out.Items) >= q.Limit || (len(out.Items) > 0 && bytes+len(encoded) > storage.AuditPageBytes) {
			out.More = true
			break
		}
		out.Items = append(out.Items, h)
		bytes += len(encoded)
	}
	if err := rows.Err(); err != nil {
		out.UnavailableUnits = []string{"postgres-canonical"}
		return out, err
	}
	return out, nil
}

func (s audit) References(ctx context.Context, source string, visit func(string) error) ([]string, error) {
	where := `COALESCE(attributes->>'withdrawn', 'false') <> 'true'`
	args := []any{}
	if source != "" {
		where += ` AND source = $1`
		args = append(args, source)
	}
	rows, err := s.db.Pool.Query(ctx, `SELECT source_ref_id FROM hazard_events WHERE `+where+` ORDER BY source_ref_id COLLATE "C", hazard_id`, args...)
	if err != nil {
		return []string{"postgres-canonical"}, err
	}
	defer rows.Close()
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return []string{"postgres-canonical"}, err
		}
		if err := visit(ref); err != nil {
			return []string{"postgres-canonical"}, err
		}
	}
	if err := rows.Err(); err != nil {
		return []string{"postgres-canonical"}, err
	}
	return nil, nil
}

var _ storage.AuditReader = audit{}
