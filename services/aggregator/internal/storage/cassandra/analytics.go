package cassandra

import (
	"context"
	"fmt"
	"sync"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func (s audit) analyticsShard(ctx context.Context, scope string, shard int, after string) ([]storage.AnalyticsRow, string, error) {
	ctx, cancel := context.WithTimeout(ctx, readUnitTimeout)
	defer cancel()
	iter := s.db.session.Query(`SELECT hazard_id,occurred_at,hazard_type,severity,area_name,tsunami,active
		FROM analytics_by_shard WHERE scope=? AND shard=? AND hazard_id>? LIMIT 128`, scope, shard, after).WithContext(ctx).PageSize(128).Iter()
	rows := []storage.AnalyticsRow{}
	var id, last string
	var row storage.AnalyticsRow
	var active bool
	for iter.Scan(&id, &row.OccurredAt, &row.HazardType, &row.Severity, &row.Area, &row.Tsunami, &active) {
		last = id
		if active {
			row.OccurredAt = row.OccurredAt.UTC()
			rows = append(rows, row)
		}
	}
	return rows, last, iter.Close()
}

func (s audit) ScanAnalytics(ctx context.Context, source string, visit func(storage.AnalyticsRow) error) ([]string, error) {
	units, _ := s.ready(ctx)
	if source == "" {
		source = scopeAll
	}
	jobs := make(chan int, auditShards)
	type result struct {
		rows []storage.AnalyticsRow
		unit string
		err  error
	}
	results := make(chan result, 4)
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for shard := range jobs {
				after := ""
				for ctx.Err() == nil {
					rows, last, err := s.analyticsShard(ctx, source, shard, after)
					r := result{rows: rows, err: err}
					if err != nil {
						r.unit = fmt.Sprintf("analytics-shard-%02d", shard)
					}
					select {
					case results <- r:
					case <-ctx.Done():
						return
					}
					if err != nil || last == "" {
						break
					}
					after = last
				}
			}
		}()
	}
	for shard := range auditShards {
		jobs <- shard
	}
	close(jobs)
	go func() { workers.Wait(); close(results) }()
	var failure error
	for r := range results {
		if r.unit != "" {
			units = append(units, r.unit)
		}
		for _, row := range r.rows {
			if failure == nil {
				failure = visit(row)
			}
		}
	}
	if ctx.Err() != nil {
		units = append(units, "analytics-deadline")
		failure = ctx.Err()
	}
	return uniqueUnits(units), failure
}

var _ storage.AnalyticsReader = audit{}
