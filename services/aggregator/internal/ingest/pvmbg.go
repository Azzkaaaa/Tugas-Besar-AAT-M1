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

type PVMBGPoller struct {
	cfg    *config.Config
	db     *store.DB
	client *http.Client
}

func NewPVMBGPoller(cfg *config.Config, db *store.DB) *PVMBGPoller {
	return &PVMBGPoller{
		cfg: cfg,
		db:  db,
		client: &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
			Timeout:   cfg.PVMBGRequestTimeout,
		},
	}
}

func (p *PVMBGPoller) Start(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PVMBGPollInterval)
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

func (p *PVMBGPoller) poll(ctx context.Context) {
	corrID := "poll-pvmbg-" + uuid.New().String()
	pollStart := time.Now()

	var ctxPoll context.Context
	var span trace.Span
	if telemetry.Tracer != nil {
		ctxPoll, span = telemetry.Tracer.Start(ctx, "pvmbg.poll")
		defer span.End()
	} else {
		ctxPoll = ctx
	}

	recordPollMetrics := func(err error, count int) {
		durationSec := time.Since(pollStart).Seconds()
		if telemetry.IngestDurationHistogram != nil {
			telemetry.IngestDurationHistogram.Record(ctxPoll, durationSec, metric.WithAttributes(attribute.String("source", "PVMBG")))
		}
		if err != nil {
			if telemetry.IngestErrorsCounter != nil {
				telemetry.IngestErrorsCounter.Add(ctxPoll, 1, metric.WithAttributes(attribute.String("source", "PVMBG")))
			}
		} else if count > 0 && telemetry.HazardsIngestedCounter != nil {
			telemetry.HazardsIngestedCounter.Add(ctxPoll, int64(count), metric.WithAttributes(attribute.String("source", "PVMBG")))
		}
	}

	_, watermark, err := p.db.GetWatermark(ctxPoll, "pvmbg_volcanic")
	if err != nil {
		watermark = time.Time{}
	}

	reports, err := p.fetchVolcanicReports(ctxPoll, watermark, corrID)
	if err != nil {
		recordPollMetrics(err, 0)
		_ = p.db.UpdateWatermark(ctxPoll, "pvmbg_volcanic", "PVMBG", watermark, false, err.Error())
		return
	}

	tx, err := p.db.Pool.Begin(ctxPoll)
	if err != nil {
		recordPollMetrics(err, 0)
		_ = p.db.UpdateWatermark(ctxPoll, "pvmbg_volcanic", "PVMBG", watermark, false, err.Error())
		return
	}
	defer tx.Rollback(ctxPoll)

	newWatermark := watermark
	newCount := 0
	for _, r := range reports {
		h := MapPVMBGToHazard(r)

		savedHazard, isNewOrUpdated, err := p.db.UpsertHazard(ctxPoll, tx, h)
		if err != nil {
			recordPollMetrics(err, 0)
			_ = p.db.UpdateWatermark(ctxPoll, "pvmbg_volcanic", "PVMBG", watermark, false, err.Error())
			return
		}

		if isNewOrUpdated {
			newCount++
			pubID, payload, err := BuildPublishedEnvelope(savedHazard, corrID)
			if err == nil {
				_ = p.db.EnqueueOutbox(ctxPoll, tx, pubID, savedHazard.HazardID, "PVMBG", payload)
			}
		}

		if r.ReportedAt.After(newWatermark) {
			newWatermark = r.ReportedAt
		}
	}

	if err := tx.Commit(ctxPoll); err != nil {
		recordPollMetrics(err, 0)
		_ = p.db.UpdateWatermark(ctxPoll, "pvmbg_volcanic", "PVMBG", watermark, false, err.Error())
		return
	}

	_ = p.db.UpdateWatermark(ctxPoll, "pvmbg_volcanic", "PVMBG", newWatermark, true, "")
	recordPollMetrics(nil, newCount)
}

func (p *PVMBGPoller) fetchVolcanicReports(ctx context.Context, since time.Time, corrID string) ([]*PVMBGVolcanicRaw, error) {
	reqURL := fmt.Sprintf("%s/volcanic-reports", p.cfg.PVMBGBaseURL)
	if !since.IsZero() {
		reqURL += "?since=" + url.QueryEscape(since.Format(time.RFC3339))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+p.cfg.PVMBGBearerToken)
	req.Header.Set("X-Correlation-ID", corrID)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("PVMBG unreachable or timeout: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("PVMBG volcanic endpoint returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	type listResponse struct {
		Items []*PVMBGVolcanicRaw `json:"items"`
	}
	var res listResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("failed decoding PVMBG response: %w", err)
	}
	return res.Items, nil
}
