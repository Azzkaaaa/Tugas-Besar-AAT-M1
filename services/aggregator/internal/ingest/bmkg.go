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
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type BMKGPoller struct {
	cfg     *config.Config
	hazards storage.HazardStore
	sources storage.SourceStateStore
	client  *http.Client
}

func NewBMKGPoller(cfg *config.Config, hazards storage.HazardStore, sources storage.SourceStateStore) *BMKGPoller {
	return &BMKGPoller{
		cfg:     cfg,
		hazards: hazards,
		sources: sources,
		client: &http.Client{
			Transport: telemetry.ExternalTransport(http.DefaultTransport),
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

	var watermark time.Time
	if state, err := p.sources.Get(ctxPoll, domain.SourceKeyBMKGSeismic); err == nil {
		watermark = state.Watermark
	}
	fail := func(err error) {
		recordPollMetrics(err, 0)
		_ = p.sources.RecordFailure(ctxPoll, domain.SourceKeyBMKGSeismic, err.Error())
	}

	seismicEvents, err := p.fetchSeismicEvents(ctxPoll, watermark, corrID)
	if err != nil {
		fail(err)
		return
	}

	tsunamiWarnings, _ := p.fetchTsunamiWarnings(ctxPoll, watermark, corrID)

	// Index warnings by related_event_id
	warningMap := make(map[string][]*BMKGTsunamiWarningRaw)
	for _, w := range tsunamiWarnings {
		warningMap[w.RelatedEventID] = append(warningMap[w.RelatedEventID], w)
	}

	records := make([]*domain.HazardRecord, 0, len(seismicEvents))
	for _, s := range seismicEvents {
		records = append(records, MapBMKGToHazard(s, warningMap[s.EventID]))
	}
	newCount, newWatermark, err := ingestRecords(ctxPoll, p.hazards, records, watermark, corrID)
	if err != nil {
		fail(err)
		return
	}

	_ = p.sources.RecordSuccess(ctxPoll, domain.SourceKeyBMKGSeismic, newWatermark)
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
