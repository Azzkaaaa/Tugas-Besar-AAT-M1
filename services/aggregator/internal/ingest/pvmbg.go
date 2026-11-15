package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	cfg      *config.Config
	hazards  storage.HazardStore
	sources  storage.SourceStateStore
	client   *http.Client
	pipeline *Pipeline
}

func (p *PVMBGPoller) SetPipeline(pipeline *Pipeline) { p.pipeline = pipeline }

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
	state, stateErr := p.sources.Get(ctxPoll, domain.SourceKeyPVMBGVolcanic)
	if stateErr == nil {
		watermark = state.Watermark
	}
	fail := func(err error) {
		recordPollMetrics(err, 0)
		_ = p.sources.RecordFailure(ctxPoll, domain.SourceKeyPVMBGVolcanic, err.Error())
	}
	if p.pipeline != nil {
		if stateErr != nil {
			fail(stateErr)
			return nil
		}
		response, err := p.volcanicResponse(ctxPoll, watermark, corrID)
		var latest time.Time
		if err == nil {
			var received incoming
			received, err = p.pipeline.receive(ctxPoll, response, domain.SourceKeyPVMBGVolcanic, formatVolcanic, corrID)
			if err == nil {
				latest, err = p.pipeline.stage(ctxPoll, received, nil)
			}
		}
		count, processErr := p.pipeline.process(ctxPoll, domain.SourceKeyPVMBGVolcanic)
		if err != nil {
			fail(err)
			return err
		}
		if processErr != nil {
			fail(processErr)
			return nil
		}
		if latest.After(watermark) {
			watermark = latest
		}
		if err := p.sources.RecordSuccess(ctxPoll, domain.SourceKeyPVMBGVolcanic, watermark); err != nil {
			fail(err)
			return nil
		}
		recordPollMetrics(nil, count)
		return nil
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
	resp, err := p.volcanicResponse(ctx, since, corrID)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	type listResponse struct {
		Items []*PVMBGVolcanicRaw `json:"items"`
	}
	var res listResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("failed decoding PVMBG response: %w", err)
	}
	return res.Items, nil
}

func (p *PVMBGPoller) volcanicResponse(ctx context.Context, since time.Time, corrID string) (*http.Response, error) {
	return fetchSource(ctx, p.client, p.cfg.PVMBGBaseURL, "/volcanic-reports", "Authorization", "Bearer "+p.cfg.PVMBGBearerToken, since, corrID)
}
