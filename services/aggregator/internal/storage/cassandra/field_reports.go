package cassandra

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const problemShards = storage.IngestShardCount

func problemShard(id string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(id))
	return int(hash.Sum32() % problemShards)
}

type fieldReports struct{ db *DB }

func (s fieldReports) Accept(ctx context.Context, report storage.FieldReport) error {
	observed := report.ObservedAt.UTC().Format(time.RFC3339Nano)
	applied, err := s.db.session.Query(`INSERT INTO field_reports
		(shard, report_id, actor, observed_at, area_name, description, received_at, correlation_id, processed)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, false) IF NOT EXISTS`, problemShard(report.ID), report.ID,
		report.Actor, observed, report.AreaName, report.Description, report.ReceivedAt, report.CorrelationID).
		WithContext(ctx).Consistency(gocql.LocalQuorum).MapScanCAS(map[string]any{})
	if err != nil || applied {
		return err
	}
	prior, err := s.readReport(ctx, report.ID, false)
	if err != nil {
		return err
	}
	if prior == nil {
		return errors.New("existing report could not be confirmed")
	}
	if prior.Actor != report.Actor || !prior.ObservedAt.Equal(report.ObservedAt) || prior.AreaName != report.AreaName || prior.Description != report.Description {
		return storage.ErrReportConflict
	}
	return nil
}

func (s fieldReports) GetReport(ctx context.Context, id string) (*storage.FieldReport, error) {
	return s.readReport(ctx, id, true)
}

func (s fieldReports) readReport(ctx context.Context, id string, processedView bool) (*storage.FieldReport, error) {
	report := storage.FieldReport{ID: id}
	var observed string
	var processed *time.Time
	var area, description string
	err := s.db.session.Query(`SELECT actor, observed_at, area_name, description, received_at, processed_at,
		processed_area_name, processed_description, correlation_id
		FROM field_reports WHERE shard = ? AND report_id = ?`, problemShard(id), id).
		WithContext(ctx).Consistency(gocql.LocalQuorum).
		Scan(&report.Actor, &observed, &report.AreaName, &report.Description, &report.ReceivedAt, &processed, &area, &description, &report.CorrelationID)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	report.ObservedAt, err = time.Parse(time.RFC3339Nano, observed)
	report.ReceivedAt = report.ReceivedAt.UTC()
	if processed != nil {
		report.ProcessedAt = processed.UTC()
		if processedView {
			report.AreaName, report.Description = area, description
		}
	}
	return &report, err
}

func (s fieldReports) ProcessReports(ctx context.Context) (int, error) {
	count := 0
	for shard := range problemShards {
		iter := s.db.session.Query(`SELECT report_id, processed FROM field_reports WHERE shard = ?`, shard).
			WithContext(ctx).Consistency(gocql.LocalQuorum).PageSize(64).Iter()
		var id string
		var processed bool
		for iter.Scan(&id, &processed) {
			if processed {
				continue
			}
			applied, err := s.complete(ctx, shard, id)
			if err != nil {
				_ = iter.Close()
				return count, err
			}
			if applied {
				count++
			}
		}
		if err := iter.Close(); err != nil {
			return count, err
		}
	}
	return count, nil
}

func (s fieldReports) complete(ctx context.Context, shard int, id string) (bool, error) {
	report, err := s.readReport(ctx, id, false)
	if err != nil || report == nil || !report.ProcessedAt.IsZero() {
		return false, err
	}
	area := strings.Join(strings.Fields(report.AreaName), " ")
	description := strings.TrimSpace(report.Description)
	// The conditional update commits the normalized output and terminal state together.
	processed := time.Now().UTC()
	applied, err := s.db.session.Query(`UPDATE field_reports SET processed = true, processed_at = ?,
		processed_area_name = ?, processed_description = ? WHERE shard = ? AND report_id = ? IF processed = false`,
		processed, area, description, shard, id).
		WithContext(ctx).Consistency(gocql.LocalQuorum).MapScanCAS(map[string]any{})
	if err == nil && applied {
		slog.InfoContext(ctx, "field report processed", "report_id", id, "correlation_id", report.CorrelationID, "processed_at", processed)
	}
	return applied, err
}
