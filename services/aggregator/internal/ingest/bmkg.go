package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/store"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type BMKGPoller struct {
	cfg    *config.Config
	db     *store.DB
	client *http.Client
}

func NewBMKGPoller(cfg *config.Config, db *store.DB) *BMKGPoller {
	return &BMKGPoller{
		cfg: cfg,
		db:  db,
		client: &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
			Timeout:   cfg.BMKGRequestTimeout,
		},
	}
}

func (p *BMKGPoller) Start(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.BMKGPollInterval)
	defer ticker.Stop()

	// Initial poll immediately
	p.poll(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *BMKGPoller) poll(ctx context.Context) {
	corrID := "poll-bmkg-" + uuid.New().String()
	pollStart := time.Now()

	var ctxPoll context.Context
	var span trace.Span
	if telemetry.Tracer != nil {
		ctxPoll, span = telemetry.Tracer.Start(ctx, "bmkg.poll")
		defer span.End()
	} else {
		ctxPoll = ctx
	}

	recordPollMetrics := func(err error, count int) {
		durationSec := time.Since(pollStart).Seconds()
		if telemetry.IngestDurationHistogram != nil {
			telemetry.IngestDurationHistogram.Record(ctxPoll, durationSec, metric.WithAttributes(attribute.String("source", "BMKG")))
		}
		if err != nil {
			if telemetry.IngestErrorsCounter != nil {
				telemetry.IngestErrorsCounter.Add(ctxPoll, 1, metric.WithAttributes(attribute.String("source", "BMKG")))
			}
		} else if count > 0 && telemetry.HazardsIngestedCounter != nil {
			telemetry.HazardsIngestedCounter.Add(ctxPoll, int64(count), metric.WithAttributes(attribute.String("source", "BMKG")))
		}
	}

	_, watermark, err := p.db.GetWatermark(ctxPoll, "bmkg_seismic")
	if err != nil {
		watermark = time.Time{}
	}

	seismicEvents, err := p.fetchSeismicEvents(ctxPoll, watermark, corrID)
	if err != nil {
		recordPollMetrics(err, 0)
		_ = p.db.UpdateWatermark(ctxPoll, "bmkg_seismic", "BMKG", watermark, false, err.Error())
		return
	}

	tsunamiWarnings, _ := p.fetchTsunamiWarnings(ctxPoll, watermark, corrID)

	// Index warnings by related_event_id
	warningMap := make(map[string][]*BMKGTsunamiWarningRaw)
	for _, w := range tsunamiWarnings {
		warningMap[w.RelatedEventID] = append(warningMap[w.RelatedEventID], w)
	}

	tx, err := p.db.Pool.Begin(ctxPoll)
	if err != nil {
		recordPollMetrics(err, 0)
		_ = p.db.UpdateWatermark(ctxPoll, "bmkg_seismic", "BMKG", watermark, false, err.Error())
		return
	}
	defer tx.Rollback(ctxPoll)

	newWatermark := watermark
	newCount := 0
	for _, s := range seismicEvents {
		relatedWarnings := warningMap[s.EventID]
		h := MapBMKGToHazard(s, relatedWarnings)

		savedHazard, isNewOrUpdated, err := p.db.UpsertHazard(ctxPoll, tx, h)
		if err != nil {
			recordPollMetrics(err, 0)
			_ = p.db.UpdateWatermark(ctxPoll, "bmkg_seismic", "BMKG", watermark, false, err.Error())
			return
		}

		if isNewOrUpdated {
			newCount++
			pubID, payload, err := BuildPublishedEnvelope(savedHazard, corrID)
			if err == nil {
				_ = p.db.EnqueueOutbox(ctxPoll, tx, pubID, savedHazard.HazardID, "BMKG", payload)
			}
		}

		if s.OccurredAt.After(newWatermark) {
			newWatermark = s.OccurredAt
		}
	}

	if err := tx.Commit(ctxPoll); err != nil {
		recordPollMetrics(err, 0)
		_ = p.db.UpdateWatermark(ctxPoll, "bmkg_seismic", "BMKG", watermark, false, err.Error())
		return
	}

	_ = p.db.UpdateWatermark(ctxPoll, "bmkg_seismic", "BMKG", newWatermark, true, "")
	recordPollMetrics(nil, newCount)
}

func (p *BMKGPoller) fetchSeismicEvents(ctx context.Context, since time.Time, corrID string) ([]*BMKGSeismicRaw, error) {
	reqURL := fmt.Sprintf("%s/seismic-events", p.cfg.BMKGBaseURL)
	if !since.IsZero() {
		reqURL += "?since=" + url.QueryEscape(since.Format(time.RFC3339))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-BMKG-Key", p.cfg.BMKGAPIKey)
	req.Header.Set("X-Correlation-ID", corrID)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("BMKG seismic endpoint returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	type listResponse struct {
		Items []*BMKGSeismicRaw `json:"items"`
	}
	var res listResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res.Items, nil
}

func (p *BMKGPoller) fetchTsunamiWarnings(ctx context.Context, since time.Time, corrID string) ([]*BMKGTsunamiWarningRaw, error) {
	reqURL := fmt.Sprintf("%s/tsunami-warnings", p.cfg.BMKGBaseURL)
	if !since.IsZero() {
		reqURL += "?since=" + url.QueryEscape(since.Format(time.RFC3339))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-BMKG-Key", p.cfg.BMKGAPIKey)
	req.Header.Set("X-Correlation-ID", corrID)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("BMKG tsunami warning returned HTTP %d", resp.StatusCode)
	}

	type listResponse struct {
		Items []*BMKGTsunamiWarningRaw `json:"items"`
	}
	var res listResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res.Items, nil
}
