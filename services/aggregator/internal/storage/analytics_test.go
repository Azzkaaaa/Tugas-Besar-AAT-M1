package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

type analyticsFixture struct {
	rows  []AnalyticsRow
	units []string
	err   error
}

func (f analyticsFixture) ScanAnalytics(_ context.Context, _ string, visit func(AnalyticsRow) error) ([]string, error) {
	for _, row := range f.rows {
		if err := visit(row); err != nil {
			return nil, err
		}
	}
	return f.units, f.err
}

func TestAnalyticsCountsAllRowsAndOverlappingTsunami(t *testing.T) {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	rows := []AnalyticsRow{}
	for i := range 1500 {
		rows = append(rows, AnalyticsRow{OccurredAt: base.Add(time.Duration(i%3) * 24 * time.Hour), HazardType: []string{"SEISMIC", "VOLCANIC", "WILDFIRE", "COASTAL"}[i%4], Severity: []string{"NORMAL", "WASPADA", "SIAGA", "AWAS"}[i%4], Area: []string{"Padang", "Bali"}[i%2], Tsunami: i%8 == 0})
	}
	result := Aggregate(context.Background(), analyticsFixture{rows: rows}, AnalyticsQuery{Since: base.Add(24 * time.Hour), Until: base.Add(72 * time.Hour), Interval: "day"})
	if result.Total != 1000 || result.Categories["SEISMIC"] != 250 || result.Categories["TSUNAMI"] != 125 || len(result.Series) != 2 || len(result.Areas) != 2 || result.Partial {
		t.Fatalf("incorrect aggregate: %+v", result)
	}
	var severity, area int64
	for _, n := range result.Severity {
		severity += n
	}
	for _, row := range result.Areas {
		area += row.Count
	}
	if severity != result.Total || area != result.Total {
		t.Fatal("aggregate dimensions do not reconcile")
	}
	partial := Aggregate(context.Background(), analyticsFixture{rows: rows[:7], units: []string{"analytics-shard-03"}, err: errors.New("node down")}, AnalyticsQuery{})
	if !partial.Partial || partial.Total != 7 || len(partial.UnavailableUnits) != 1 {
		t.Fatalf("partial data discarded: %+v", partial)
	}
}

func TestTsunamiCountingRule(t *testing.T) {
	for _, warnings := range []any{[]map[string]any{{"area": "Padang"}, {"area": "Bali"}}, []any{map[string]any{"area": "Padang"}}} {
		h := &domain.HazardRecord{HazardType: "SEISMIC", Attributes: map[string]any{"tsunami_warnings": warnings}}
		if !HasTsunami(h) {
			t.Fatal("nonempty warning array not counted")
		}
		h.HazardType = "COASTAL"
		if HasTsunami(h) {
			t.Fatal("coastal observation counted as predicted tsunami")
		}
	}
	if HasTsunami(&domain.HazardRecord{HazardType: "SEISMIC", Attributes: map[string]any{"tsunami_warnings": []any{}}}) {
		t.Fatal("empty warning array counted")
	}
}
