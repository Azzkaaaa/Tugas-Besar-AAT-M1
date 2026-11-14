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

type PVMBGPoller struct {
	cfg     *config.Config
	hazards storage.HazardStore
	sources storage.SourceStateStore
	client  *http.Client
}

func NewPVMBGPoller(cfg *config.Config, hazards storage.HazardStore, sources storage.SourceStateStore) *PVMBGPoller {
	return &PVMBGPoller{
		cfg:     cfg,
		hazards: hazards,
		sources: sources,
		client: &http.Client{
			Transport: telemetry.ExternalTransport(http.DefaultTransport),
			Timeout:   cfg.PVMBGRequestTimeout,
		},
	}
}

func (p *PVMBGPoller) Start(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PVMBGPollInterval)
	defer ticker.Stop()

	// Initial poll immediately. After each poll (a safe point) the poller may hand its lease
	// over: released when the upstream has been unreachable from here, or yielded to rebalance.
	guard := newLeaseGuard("PVMBG", p.cfg.Leader.UnreachableAfter)
	if guard.after(ctx, p.poll(ctx)) {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if guard.after(ctx, p.poll(ctx)) {
				return
			}
		}
	}
}

// poll runs one poll and returns the error of the upstream fetch (nil when the upstream answered,
// even if storing the result failed).
func (p *PVMBGPoller) poll(ctx context.Context) error {
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

	var watermark time.Time
	if state, err := p.sources.Get(ctxPoll, domain.SourceKeyPVMBGVolcanic); err == nil {
		watermark = state.Watermark
	}
	fail := func(err error) {
		recordPollMetrics(err, 0)
		_ = p.sources.RecordFailure(ctxPoll, domain.SourceKeyPVMBGVolcanic, err.Error())
	}

	reports, err := p.fetchVolcanicReports(ctxPoll, watermark, corrID)
	if err != nil {
		fail(err)
		return err
	}

	records := make([]*domain.HazardRecord, 0, len(reports))
	for _, r := range reports {
		records = append(records, MapPVMBGToHazard(r))
	}
	newCount, newWatermark, err := ingestRecords(ctxPoll, p.hazards, records, watermark, corrID)
	if err != nil {
		fail(err)
		return nil
	}

	_ = p.sources.RecordSuccess(ctxPoll, domain.SourceKeyPVMBGVolcanic, newWatermark)
	recordPollMetrics(nil, newCount)
	return nil
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
