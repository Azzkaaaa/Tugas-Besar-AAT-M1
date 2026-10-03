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
			Timeout: cfg.PVMBGRequestTimeout,
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

	_, watermark, err := p.db.GetWatermark(ctx, "pvmbg_volcanic")
	if err != nil {
		watermark = time.Time{}
	}

	reports, err := p.fetchVolcanicReports(ctx, watermark, corrID)
	if err != nil {
		_ = p.db.UpdateWatermark(ctx, "pvmbg_volcanic", "PVMBG", watermark, false, err.Error())
		return
	}

	tx, err := p.db.Pool.Begin(ctx)
	if err != nil {
		_ = p.db.UpdateWatermark(ctx, "pvmbg_volcanic", "PVMBG", watermark, false, err.Error())
		return
	}
	defer tx.Rollback(ctx)

	newWatermark := watermark
	for _, r := range reports {
		h := MapPVMBGToHazard(r)

		savedHazard, isNewOrUpdated, err := p.db.UpsertHazard(ctx, tx, h)
		if err != nil {
			_ = p.db.UpdateWatermark(ctx, "pvmbg_volcanic", "PVMBG", watermark, false, err.Error())
			return
		}

		if isNewOrUpdated {
			pubID, payload, err := BuildPublishedEnvelope(savedHazard, corrID)
			if err == nil {
				_ = p.db.EnqueueOutbox(ctx, tx, pubID, savedHazard.HazardID, "PVMBG", payload)
			}
		}

		if r.ReportedAt.After(newWatermark) {
			newWatermark = r.ReportedAt
		}
	}

	if err := tx.Commit(ctx); err != nil {
		_ = p.db.UpdateWatermark(ctx, "pvmbg_volcanic", "PVMBG", watermark, false, err.Error())
		return
	}

	_ = p.db.UpdateWatermark(ctx, "pvmbg_volcanic", "PVMBG", newWatermark, true, "")
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
