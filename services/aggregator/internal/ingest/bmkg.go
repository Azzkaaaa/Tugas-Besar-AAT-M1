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
	"github.com/google/uuid"
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
			Timeout: cfg.BMKGRequestTimeout,
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

	_, watermark, err := p.db.GetWatermark(ctx, "bmkg_seismic")
	if err != nil {
		watermark = time.Time{}
	}

	seismicEvents, err := p.fetchSeismicEvents(ctx, watermark, corrID)
	if err != nil {
		_ = p.db.UpdateWatermark(ctx, "bmkg_seismic", "BMKG", watermark, false, err.Error())
		return
	}

	tsunamiWarnings, _ := p.fetchTsunamiWarnings(ctx, watermark, corrID)

	// Index warnings by related_event_id
	warningMap := make(map[string][]*BMKGTsunamiWarningRaw)
	for _, w := range tsunamiWarnings {
		warningMap[w.RelatedEventID] = append(warningMap[w.RelatedEventID], w)
	}

	tx, err := p.db.Pool.Begin(ctx)
	if err != nil {
		_ = p.db.UpdateWatermark(ctx, "bmkg_seismic", "BMKG", watermark, false, err.Error())
		return
	}
	defer tx.Rollback(ctx)

	newWatermark := watermark
	for _, s := range seismicEvents {
		relatedWarnings := warningMap[s.EventID]
		h := MapBMKGToHazard(s, relatedWarnings)

		savedHazard, isNewOrUpdated, err := p.db.UpsertHazard(ctx, tx, h)
		if err != nil {
			_ = p.db.UpdateWatermark(ctx, "bmkg_seismic", "BMKG", watermark, false, err.Error())
			return
		}

		if isNewOrUpdated {
			pubID, payload, err := BuildPublishedEnvelope(savedHazard, corrID)
			if err == nil {
				_ = p.db.EnqueueOutbox(ctx, tx, pubID, savedHazard.HazardID, "BMKG", payload)
			}
		}

		if s.OccurredAt.After(newWatermark) {
			newWatermark = s.OccurredAt
		}
	}

	if err := tx.Commit(ctx); err != nil {
		_ = p.db.UpdateWatermark(ctx, "bmkg_seismic", "BMKG", watermark, false, err.Error())
		return
	}

	_ = p.db.UpdateWatermark(ctx, "bmkg_seismic", "BMKG", newWatermark, true, "")
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

	var events []*BMKGSeismicRaw
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		return nil, err
	}
	return events, nil
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

	var warnings []*BMKGTsunamiWarningRaw
	if err := json.NewDecoder(resp.Body).Decode(&warnings); err != nil {
		return nil, err
	}
	return warnings, nil
}
