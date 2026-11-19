package m2

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type ReplayOptions struct {
	WorkDir             string
	Lateness, Retention time.Duration
	MaxDetections       int
}

type ReplaySummary struct {
	InputEvents          int64   `json:"input_events"`
	ActiveFires          int     `json:"active_fires"`
	Singletons           int     `json:"singletons"`
	ExcludedLow          int64   `json:"excluded_low"`
	Late                 int64   `json:"late"`
	ExpiredCorrections   int64   `json:"expired_corrections"`
	OverCapacity         int64   `json:"over_capacity"`
	PeakActiveDetections int     `json:"peak_active_detections"`
	ParkedEvents         int64   `json:"parked_events"`
	Retention            string  `json:"retention"`
	ProcessingSeconds    float64 `json:"processing_seconds"`
	P95ComputeSeconds    float64 `json:"p95_compute_seconds"`
	Engine               string  `json:"engine"`
	Measurement          string  `json:"measurement"`
}

// ReplayJournal uses the exact production disk processor. Graph state, final
// results, exact duration samples and distinct rejection reasons remain on disk.
// Memory is bounded by SQL caches and the current journal line/output record.
func ReplayJournal(ctx context.Context, input io.Reader, output io.Writer, options ReplayOptions) (summary ReplaySummary, err error) {
	d, err := newDiskFireProcessor(ctx, options.WorkDir, options.Lateness, options.Retention, options.MaxDetections)
	if err != nil {
		return summary, err
	}
	defer func() { err = errors.Join(err, d.Close()) }()
	d.keepFinal = true
	if _, err = d.graph.db.ExecContext(ctx, `CREATE TABLE replay_timings(ns INTEGER NOT NULL);
		CREATE INDEX replay_duration ON replay_timings(ns);
		CREATE TABLE replay_reasons(reason TEXT PRIMARY KEY, count INTEGER NOT NULL);`); err != nil {
		return summary, err
	}
	s := bufio.NewScanner(input)
	s.Buffer(make([]byte, 4096), 4<<20)
	started := time.Now()
	for s.Scan() {
		var row storage.StreamEntry
		if err := json.Unmarshal(s.Bytes(), &row); err != nil {
			return summary, fmt.Errorf("journal row %d: %w", summary.InputEvents+1, err)
		}
		at := time.Now()
		applyErr := d.ApplyTo(ctx, row.Kind, row.Payload, row.Sequence, row.ReceivedAt, nil)
		elapsed := time.Since(at).Nanoseconds()
		if applyErr != nil && !errors.Is(applyErr, errFireInput) {
			return summary, fmt.Errorf("journal sequence %d: %w", row.Sequence, applyErr)
		}
		if _, err := d.graph.db.ExecContext(ctx, `INSERT INTO replay_timings VALUES(?)`, elapsed); err != nil {
			return summary, err
		}
		if applyErr != nil {
			if _, err := d.graph.db.ExecContext(ctx, `INSERT INTO replay_reasons VALUES(?,1) ON CONFLICT(reason) DO UPDATE SET count=count+1`, applyErr.Error()); err != nil {
				return summary, err
			}
			summary.ParkedEvents++
		}
		summary.PeakActiveDetections = max(summary.PeakActiveDetections, d.Active())
		summary.InputEvents++
	}
	if err := s.Err(); err != nil {
		return summary, err
	}
	if summary.InputEvents > 0 {
		var ns int64
		if err := d.graph.db.QueryRowContext(ctx, `SELECT ns FROM replay_timings ORDER BY ns LIMIT 1 OFFSET ?`, (summary.InputEvents-1)*95/100).Scan(&ns); err != nil {
			return summary, err
		}
		summary.P95ComputeSeconds = float64(ns) / float64(time.Second)
	}
	summary.ActiveFires, summary.Singletons = d.Counts()
	summary.ExcludedLow, summary.Late = d.meta.Low, d.meta.Late
	summary.ExpiredCorrections, summary.OverCapacity = d.meta.Expired, d.meta.OverCapacity
	summary.Retention = d.meta.Retention.String()
	summary.ProcessingSeconds = time.Since(started).Seconds()
	summary.Engine = "production-disk"
	summary.Measurement = "offline disk processor only; not ingest-to-canonical-storage or notification latency"
	header, err := json.Marshal(summary)
	if err != nil {
		return summary, err
	}
	if _, err = output.Write(header[:len(header)-1]); err != nil {
		return summary, err
	}
	if _, err = io.WriteString(output, `,"parked_reasons":{`); err != nil {
		return summary, err
	}
	if err := writeReplayReasons(ctx, d, output); err != nil {
		return summary, err
	}
	if _, err = io.WriteString(output, `},"items":[`); err != nil {
		return summary, err
	}
	first := true
	encoder := json.NewEncoder(output)
	if err := d.VisitRecords(ctx, func(record *domain.HazardRecord) error {
		if !first {
			if _, err := io.WriteString(output, ","); err != nil {
				return err
			}
		}
		first = false
		return encoder.Encode(record)
	}); err != nil {
		return summary, err
	}
	_, err = io.WriteString(output, "]}\n")
	return summary, err
}

func writeReplayReasons(ctx context.Context, d *diskFireProcessor, output io.Writer) error {
	rows, err := d.graph.db.QueryContext(ctx, `SELECT reason,count FROM replay_reasons ORDER BY reason`)
	if err != nil {
		return err
	}
	defer rows.Close()
	first := true
	for rows.Next() {
		var reason string
		var count int64
		if err := rows.Scan(&reason, &count); err != nil {
			return err
		}
		if !first {
			if _, err := io.WriteString(output, ","); err != nil {
				return err
			}
		}
		first = false
		key, err := json.Marshal(reason)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(output, "%s:%d", key, count); err != nil {
			return err
		}
	}
	return rows.Err()
}
