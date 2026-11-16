package m2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Station struct {
	ID, Province        string
	Latitude, Longitude float64
}
type coastalPage struct {
	Body     []byte             `json:"body"`
	Stations map[string]Station `json:"stations"`
}

func parseStations(raw []byte) (map[string]Station, error) {
	o, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	var items []json.RawMessage
	if err = json.Unmarshal(o["items"], &items); err != nil || items == nil {
		return nil, errors.New("invalid_stations")
	}
	out := map[string]Station{}
	for _, raw := range items {
		o, err := decodeObject(raw)
		if err != nil {
			return nil, err
		}
		var s Station
		if s.ID, err = textField(o, "station_id"); err != nil {
			return nil, err
		}
		if s.Province, err = textField(o, "province"); err != nil {
			return nil, err
		}
		if s.Latitude, err = number(o, "latitude"); err != nil || math.Abs(s.Latitude) > 90 {
			return nil, errors.New("invalid_station_latitude")
		}
		if s.Longitude, err = number(o, "longitude"); err != nil || math.Abs(s.Longitude) > 180 {
			return nil, errors.New("invalid_station_longitude")
		}
		if _, ok := out[s.ID]; ok {
			return nil, errors.New("duplicate_station_id")
		}
		out[s.ID] = s
	}
	return out, nil
}

func observation(raw []byte, stations map[string]Station, generated, received time.Time) (*domain.HazardRecord, error) {
	o, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	id, err := textField(o, "observation_id")
	if err != nil {
		return nil, err
	}
	stationID, err := textField(o, "station_id")
	if err != nil {
		return nil, err
	}
	s, ok := stations[stationID]
	if !ok {
		return nil, errors.New("unknown_station")
	}
	at, err := timestamp(o, "observed_at")
	if err != nil {
		return nil, err
	}
	if at.After(generated.Add(30 * time.Second)) {
		return nil, errors.New("observation_in_future")
	}
	anomaly, err := number(o, "sea_level_anomaly_m")
	if err != nil {
		return nil, err
	}
	wave, err := number(o, "wave_height_m")
	if err != nil || wave < 0 {
		return nil, errors.New("invalid_wave_height")
	}
	quality, err := textField(o, "quality")
	if err != nil || quality != "ok" {
		return nil, errors.New("invalid_quality")
	}
	critical, err := boolean(o, "critical")
	if err != nil {
		return nil, err
	}
	// Appendix C supplies no numeric severity thresholds. The explicit critical
	// bit is authoritative; do not infer or override it from wave height.
	severity := domain.SeverityWaspada
	if critical {
		severity = domain.SeverityAwas
	}
	return &domain.HazardRecord{Source: domain.SourceCoastal, SourceRefID: id, HazardType: domain.HazardTypeCoastal, Severity: severity, AreaName: s.Province, Latitude: s.Latitude, Longitude: s.Longitude, OccurredAt: at, IngestedAt: received, Attributes: map[string]any{"station_id": stationID, "sea_level_anomaly_m": anomaly, "wave_height_m": wave, "quality": quality, "critical": critical}}, nil
}

func parsePage(body []byte) (time.Time, []json.RawMessage, string, error) {
	o, err := decodeObject(body)
	if err != nil {
		return time.Time{}, nil, "", err
	}
	generated, err := timestamp(o, "generated_at")
	if err != nil {
		return generated, nil, "", err
	}
	var items []json.RawMessage
	if err = json.Unmarshal(o["items"], &items); err != nil || items == nil {
		return generated, nil, "", errors.New("invalid_observation_items")
	}
	if len(items) > 1000 {
		return generated, nil, "", errors.New("too_many_observations")
	}
	var next *string
	if err = json.Unmarshal(o["next_cursor"], &next); err != nil {
		return generated, nil, "", errors.New("invalid_next_cursor")
	}
	cursor := ""
	if next != nil {
		cursor = *next
		if len(cursor) > 8192 {
			return generated, nil, "", errors.New("cursor_too_long")
		}
	}
	return generated, items, cursor, nil
}

func (r *Runner) Coastal(ctx context.Context) {
	if err := r.coastal(ctx); err != nil && ctx.Err() == nil {
		slog.ErrorContext(ctx, "coastal job restarting", "error", err, "correlation_id", "coastal-recovery")
		_ = r.Store.Sources.RecordFailure(ctx, domain.SourceKeyCoastal, err.Error())
		pause(ctx, time.Second)
	}
}
func (r *Runner) coastal(ctx context.Context) error {
	const stream = "coastal-v1"
	p, err := r.Store.Streams.Checkpoint(ctx, stream)
	if err != nil {
		return err
	}
	apply := func(e storage.StreamEntry) error {
		var page coastalPage
		if err := json.Unmarshal(e.Payload, &page); err != nil {
			return err
		}
		generated, items, _, err := parsePage(page.Body)
		if err != nil {
			return err
		}
		corr := fmt.Sprintf("coastal-%d", e.Sequence)
		work, span := otel.Tracer("bnpb.m2").Start(ctx, "coastal.process")
		defer span.End()
		span.SetAttributes(attribute.String("correlation_id", corr))
		// Prioritize real booleans, never truthy strings supplied by broken sensors.
		sort.SliceStable(items, func(i, j int) bool {
			a, _ := decodeObject(items[i])
			b, _ := decodeObject(items[j])
			x, _ := boolean(a, "critical")
			y, _ := boolean(b, "critical")
			return x && !y
		})
		for _, raw := range items {
			h, err := observation(raw, page.Stations, generated, e.ReceivedAt)
			if err != nil {
				if err = r.park(work, domain.SourceCoastal, raw, err.Error(), e.ReceivedAt); err != nil {
					return err
				}
				continue
			}
			// Observation identity deduplicates overlapping pages and reconnects;
			// only changed observation content creates a new canonical revision.
			if err = r.save(work, h, corr); err != nil {
				return err
			}
			r.metrics.latency.Record(work, time.Since(e.ReceivedAt).Seconds(), metric.WithAttributes(attribute.String("source", domain.SourceCoastal)))
		}
		if err = r.commit(work, stream, &p, e); err != nil {
			return err
		}
		r.metrics.inputs.Add(work, int64(len(items)), metric.WithAttributes(attribute.String("source", domain.SourceCoastal)))
		return nil
	}
	for {
		rows, err := r.Store.Streams.Read(ctx, stream, p.Sequence, 1)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		if err = apply(rows[0]); err != nil {
			return err
		}
	}
	c := client(r.Config.Timeout)
	defer c.CloseIdleConnections()
	c.Timeout = r.Config.Timeout
	b := Breaker{Threshold: r.Config.FailureThreshold, OpenFor: r.Config.OpenFor}
	stations := map[string]Station{}
	var stationRefresh, lastProgress, lastGenerated time.Time
	lastBody := ""
	base := strings.TrimRight(r.Config.CoastalURL, "/")
	fail := func(err error) {
		b.Fail(time.Now(), err)
		_ = r.Store.Sources.RecordFailure(ctx, domain.SourceKeyCoastal, err.Error())
		slog.WarnContext(ctx, "coastal unavailable", "error", err, "circuit_open", !b.Allow(time.Now()), "correlation_id", "coastal-poll")
	}
	for ctx.Err() == nil {
		if !b.Allow(time.Now()) {
			r.metrics.breaker.Record(ctx, 1)
			if !pause(ctx, time.Until(b.OpenUntil)) {
				break
			}
			continue
		}
		r.metrics.breaker.Record(ctx, 0)
		if len(stations) == 0 || time.Since(stationRefresh) > time.Minute {
			raw, err := get(ctx, c, base+"/v1/stations", r.Config.CoastalToken, r.Config.MaxBodyBytes)
			if err != nil {
				fail(err)
				pause(ctx, r.Config.CoastalPoll)
				continue
			}
			stations, err = parseStations(raw)
			if err != nil {
				if pe := r.park(ctx, domain.SourceCoastal, raw, err.Error(), time.Now().UTC()); pe != nil {
					return pe
				}
				fail(err)
				pause(ctx, r.Config.CoastalPoll)
				continue
			}
			stationRefresh = time.Now()
		}
		u := base + "/v1/observations?limit=200"
		if p.Cursor != "" {
			u += "&cursor=" + url.QueryEscape(p.Cursor)
		}
		raw, err := get(ctx, c, u, r.Config.CoastalToken, r.Config.MaxBodyBytes)
		received := time.Now().UTC()
		if err != nil {
			if len(raw) > 0 {
				if pe := r.park(ctx, domain.SourceCoastal, raw, err.Error()+": first bytes retained up to size cap", received); pe != nil {
					return pe
				}
			}
			fail(err)
			pause(ctx, r.Config.CoastalPoll)
			continue
		}
		generated, items, next, err := parsePage(raw)
		if err != nil {
			if pe := r.park(ctx, domain.SourceCoastal, raw, err.Error(), received); pe != nil {
				return pe
			}
			fail(err)
			pause(ctx, r.Config.CoastalPoll)
			continue
		}
		if generated.After(lastGenerated) {
			lastGenerated = generated
			lastProgress = time.Now()
		}
		// /healthz can lie. Evaluate the actual data timestamp and progress.
		// The official practice sensor uses a virtual date/speed. Comparing its
		// generated_at with the host wall clock would keep a healthy replay open forever.
		stale := !lastProgress.IsZero() && time.Since(lastProgress) > r.Config.StaleAfter
		var newest time.Time
		for _, item := range items {
			if o, err := decodeObject(item); err == nil {
				if at, err := timestamp(o, "observed_at"); err == nil && at.After(newest) {
					newest = at
				}
			}
		}
		if !newest.IsZero() && generated.Sub(newest) > r.Config.StaleAfter {
			stale = true
		}
		if next == "" {
			next = p.Cursor
		}
		advancing := next != p.Cursor
		bodyID := stableID("page-", string(raw))
		if bodyID != lastBody && len(items) > 0 {
			payload, _ := json.Marshal(coastalPage{Body: raw, Stations: stations})
			entry, err := r.Store.Streams.Append(ctx, stream, storage.StreamEntry{Sequence: p.Sequence + 1, Kind: "observations", Cursor: next, Payload: payload, ReceivedAt: received})
			if err != nil {
				return err
			}
			if err = apply(entry); err != nil {
				return err
			}
			lastBody = bodyID
		} else if next != p.Cursor {
			payload, _ := json.Marshal(coastalPage{Body: raw, Stations: stations})
			entry, err := r.Store.Streams.Append(ctx, stream, storage.StreamEntry{Sequence: p.Sequence + 1, Kind: "observations", Cursor: next, Payload: payload, ReceivedAt: received})
			if err != nil {
				return err
			}
			if err = apply(entry); err != nil {
				return err
			}
		}
		if stale {
			if advancing {
				b.Success()
				_ = r.Store.Sources.RecordFailure(ctx, domain.SourceKeyCoastal, "catching_up_stale_sensor_payload")
			} else {
				fail(errors.New("stale_sensor_payload"))
			}
		} else {
			b.Success()
			if err = r.Store.Sources.RecordSuccess(ctx, domain.SourceKeyCoastal, generated); err != nil {
				return err
			}
		}
		if advancing && len(items) >= 200 {
			continue
		}
		if !pause(ctx, r.Config.CoastalPoll) {
			break
		}
	}
	return ctx.Err()
}
