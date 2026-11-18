package storage

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

// AnalyticsRow deliberately excludes potentially large raw source attributes.
type AnalyticsRow struct {
	OccurredAt                 time.Time
	HazardType, Severity, Area string
	Tsunami                    bool
}

// AnalyticsReader visits current, non-withdrawn metadata once per hazard. The
// visitor is sequential, and successful units survive failures in other units.
type AnalyticsReader interface {
	ScanAnalytics(context.Context, string, func(AnalyticsRow) error) ([]string, error)
}

type AnalyticsQuery struct {
	Source, Interval string
	Since, Until     time.Time
}
type AnalyticsArea struct {
	AreaName string `json:"area_name"`
	Count    int64  `json:"count"`
}
type AnalyticsPoint struct {
	Bucket time.Time        `json:"bucket"`
	Counts map[string]int64 `json:"counts"`
}
type AnalyticsResult struct {
	Total            int64            `json:"total"`
	Categories       map[string]int64 `json:"categories"`
	Severity         map[string]int64 `json:"severity"`
	Areas            []AnalyticsArea  `json:"areas"`
	Series           []AnalyticsPoint `json:"series"`
	Partial          bool             `json:"partial"`
	UnavailableUnits []string         `json:"unavailable_units"`
	AsOf             time.Time        `json:"as_of"`
}

func HasTsunami(h *domain.HazardRecord) bool {
	if h.HazardType != domain.HazardTypeSeismic {
		return false
	}
	// Accept typed mapper values as well as JSON-normalized persisted slices.
	b, _ := json.Marshal(h.Attributes["tsunami_warnings"])
	var warnings []json.RawMessage
	return json.Unmarshal(b, &warnings) == nil && len(warnings) > 0
}

func analyticsCounts() map[string]int64 {
	return map[string]int64{"SEISMIC": 0, "TSUNAMI": 0, "VOLCANIC": 0, "WILDFIRE": 0, "COASTAL": 0}
}
func analyticsBucket(t time.Time, interval string) time.Time {
	t = t.UTC()
	switch interval {
	case "hour":
		return t.Truncate(time.Hour)
	case "month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	case "year":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	default:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	}
}

// Aggregate uses bounded metadata and bounded result cardinality. A catalog
// exceeding the result limits is explicitly partial, never silently truncated.
func Aggregate(ctx context.Context, reader AnalyticsReader, q AnalyticsQuery) AnalyticsResult {
	out := AnalyticsResult{Categories: analyticsCounts(), Severity: map[string]int64{"NORMAL": 0, "WASPADA": 0, "SIAGA": 0, "AWAS": 0}, Areas: []AnalyticsArea{}, Series: []AnalyticsPoint{}, UnavailableUnits: []string{}, AsOf: time.Now().UTC()}
	areas := map[string]int64{}
	series := map[time.Time]map[string]int64{}
	limited := false
	if reader == nil {
		out.Partial = true
		out.UnavailableUnits = []string{"analytics-index"}
		return out
	}
	units, err := reader.ScanAnalytics(ctx, q.Source, func(row AnalyticsRow) error {
		if (!q.Since.IsZero() && row.OccurredAt.Before(q.Since)) || (!q.Until.IsZero() && !row.OccurredAt.Before(q.Until)) {
			return nil
		}
		out.Total++
		out.Categories[row.HazardType]++
		out.Severity[row.Severity]++
		if row.Tsunami {
			out.Categories["TSUNAMI"]++
		}
		if _, ok := areas[row.Area]; ok || len(areas) < 10000 {
			areas[row.Area]++
		} else {
			limited = true
		}
		bucket := analyticsBucket(row.OccurredAt, q.Interval)
		counts, ok := series[bucket]
		if !ok && len(series) < 4096 {
			counts = analyticsCounts()
			series[bucket] = counts
		}
		if counts != nil {
			counts[row.HazardType]++
			if row.Tsunami {
				counts["TSUNAMI"]++
			}
		} else {
			limited = true
		}
		return nil
	})
	if err != nil && len(units) == 0 {
		units = append(units, "analytics-scan")
	}
	if limited {
		units = append(units, "analytics-result-capacity")
	}
	out.UnavailableUnits = append(out.UnavailableUnits, units...)
	out.Partial = len(units) > 0
	for area, count := range areas {
		out.Areas = append(out.Areas, AnalyticsArea{area, count})
	}
	sort.Slice(out.Areas, func(i, j int) bool {
		if out.Areas[i].Count == out.Areas[j].Count {
			return out.Areas[i].AreaName < out.Areas[j].AreaName
		}
		return out.Areas[i].Count > out.Areas[j].Count
	})
	for bucket, counts := range series {
		out.Series = append(out.Series, AnalyticsPoint{bucket, counts})
	}
	sort.Slice(out.Series, func(i, j int) bool { return out.Series[i].Bucket.Before(out.Series[j].Bucket) })
	return out
}
