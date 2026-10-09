package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
)

func TestBMKGPoller_FetchSeismicEvents(t *testing.T) {
	expectedAPIKey := "test-bmkg-key-123"
	expectedCorrID := "corr-test-456"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/seismic-events" {
			t.Errorf("expected path /seismic-events, got %s", r.URL.Path)
		}
		if r.Header.Get("X-BMKG-Key") != expectedAPIKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Header.Get("X-Correlation-ID") != expectedCorrID {
			t.Errorf("expected X-Correlation-ID %s, got %s", expectedCorrID, r.Header.Get("X-Correlation-ID"))
		}

		resp := map[string]interface{}{
			"items": []map[string]interface{}{
				{
					"event_id":          "eq-001",
					"magnitude":         5.4,
					"depth_km":          10.0,
					"epicenter_lat":     -7.1,
					"epicenter_lon":     110.2,
					"region_name":       "Jawa Tengah",
					"occurred_at":       "2026-10-03T00:00:00Z",
					"potential_tsunami": false,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &config.Config{
		BMKGBaseURL:        server.URL,
		BMKGAPIKey:         expectedAPIKey,
		BMKGRequestTimeout: 2 * time.Second,
	}

	poller := NewBMKGPoller(cfg, nil)
	events, err := poller.fetchSeismicEvents(context.Background(), time.Time{}, expectedCorrID)
	if err != nil {
		t.Fatalf("fetchSeismicEvents failed: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].EventID != "eq-001" || events[0].Magnitude != 5.4 {
		t.Errorf("event fields mismatch: %+v", events[0])
	}
}

func TestPVMBGPoller_FetchVolcanicReports(t *testing.T) {
	expectedToken := "test-pvmbg-token-789"
	expectedCorrID := "corr-pvmbg-test"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/volcanic-reports" {
			t.Errorf("expected path /volcanic-reports, got %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+expectedToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Header.Get("X-Correlation-ID") != expectedCorrID {
			t.Errorf("expected X-Correlation-ID %s, got %s", expectedCorrID, r.Header.Get("X-Correlation-ID"))
		}

		resp := map[string]interface{}{
			"items": []map[string]interface{}{
				{
					"report_id":           "volc-001",
					"volcano_id":          "v-merapi",
					"alert_level":         "Waspada",
					"eruption_count_24h":  5,
					"ash_column_height_m": 300.0,
					"reported_at":         "2026-10-03T01:00:00Z",
					"confidence_level":    0.88,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &config.Config{
		PVMBGBaseURL:        server.URL,
		PVMBGBearerToken:    expectedToken,
		PVMBGRequestTimeout: 2 * time.Second,
	}

	poller := NewPVMBGPoller(cfg, nil)
	reports, err := poller.fetchVolcanicReports(context.Background(), time.Time{}, expectedCorrID)
	if err != nil {
		t.Fatalf("fetchVolcanicReports failed: %v", err)
	}

	if len(reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(reports))
	}
	if reports[0].ReportID != "volc-001" || reports[0].VolcanoID != "v-merapi" {
		t.Errorf("report fields mismatch: %+v", reports[0])
	}
	if reports[0].RawFields["confidence_level"] != 0.88 {
		t.Errorf("confidence level mismatch in RawFields: %v", reports[0].RawFields["confidence_level"])
	}
}

func TestBMKGPoller_AuthRejection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	cfg := &config.Config{
		BMKGBaseURL:        server.URL,
		BMKGAPIKey:         "wrong-key",
		BMKGRequestTimeout: 1 * time.Second,
	}

	poller := NewBMKGPoller(cfg, nil)
	_, err := poller.fetchSeismicEvents(context.Background(), time.Time{}, "test")
	if err == nil {
		t.Errorf("expected error on 401 unauthorized, got nil")
	}
}
