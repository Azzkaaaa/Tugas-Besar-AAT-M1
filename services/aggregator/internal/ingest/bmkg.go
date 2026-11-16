package ingest

import (
	"context"
	"encoding/json"
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

type BMKGPoller struct {
	cfg      *config.Config
	hazards  storage.HazardStore
	sources  storage.SourceStateStore
	client   *http.Client
	pipeline *Pipeline
}

func (p *BMKGPoller) SetPipeline(pipeline *Pipeline) { p.pipeline = pipeline }

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

	// Initial poll immediately. After each poll (a safe point) the poller may hand its lease
	// over: released when the upstream has been unreachable from here, or yielded to rebalance.
	guard := newLeaseGuard("BMKG", p.cfg.Leader.UnreachableAfter)
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
func (p *BMKGPoller) poll(ctx context.Context) error {
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
	state, stateErr := p.sources.Get(ctxPoll, domain.SourceKeyBMKGSeismic)
	if stateErr == nil {
		watermark = state.Watermark
	}
	fail := func(err error) {
		recordPollMetrics(err, 0)
		_ = p.sources.RecordFailure(ctxPoll, domain.SourceKeyBMKGSeismic, err.Error())
	}
	if p.pipeline != nil {
		if stateErr != nil {
			fail(stateErr)
			return nil
		}
		latest, fetchErr := p.stageBMKG(ctxPoll, watermark, corrID)
		count, processErr := p.pipeline.process(ctxPoll, domain.SourceKeyBMKGSeismic)
		if fetchErr != nil {
			fail(fetchErr)
			return fetchErr
		}
		if processErr != nil {
			fail(processErr)
			return nil
		}
		if latest.After(watermark) {
			watermark = latest
		}
		if err := p.sources.RecordSuccess(ctxPoll, domain.SourceKeyBMKGSeismic, watermark); err != nil {
			fail(err)
			return nil
		}
		recordPollMetrics(nil, count)
		return nil
	}

	seismicEvents, err := p.fetchSeismicEvents(ctxPoll, watermark, corrID)
	if err != nil {
		fail(err)
		return err
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
		return nil
	}

	_ = p.sources.RecordSuccess(ctxPoll, domain.SourceKeyBMKGSeismic, newWatermark)
	recordPollMetrics(nil, newCount)
	return nil
}

func (p *BMKGPoller) fetchSeismicEvents(ctx context.Context, since time.Time, corrID string) ([]*BMKGSeismicRaw, error) {
	resp, err := p.bmkgResponse(ctx, "/seismic-events", since, corrID)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

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
	resp, err := p.bmkgResponse(ctx, "/tsunami-warnings", since, corrID)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	type listResponse struct {
		Items []*BMKGTsunamiWarningRaw `json:"items"`
	}
	var res listResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res.Items, nil
}

func (p *BMKGPoller) bmkgResponse(ctx context.Context, path string, since time.Time, corrID string) (*http.Response, error) {
	return fetchSource(ctx, p.client, p.cfg.BMKGBaseURL, path, "X-BMKG-Key", p.cfg.BMKGAPIKey, since, corrID)
}

func (p *BMKGPoller) stageBMKG(ctx context.Context, since time.Time, corrID string) (time.Time, error) {
	stream := domain.SourceKeyBMKGSeismic
	result := func(path, format string) (incoming, error) {
		response, err := p.bmkgResponse(ctx, path, since, corrID)
		if err != nil {
			return incoming{}, err
		}
		return p.pipeline.receive(ctx, response, stream, format, corrID)
	}
	seismic, seismicErr := result("/seismic-events", formatSeismic)
	warnings, warningErr := result("/tsunami-warnings", formatWarning)
	var latest time.Time
	if seismicErr == nil {
		latest, seismicErr = p.pipeline.stage(ctx, seismic, validWarnings(warnings))
	}
	if warningErr == nil {
		_, warningErr = p.pipeline.stage(ctx, warnings, nil)
	}
	if seismicErr != nil {
		return time.Time{}, seismicErr
	}
	return latest, warningErr
}
