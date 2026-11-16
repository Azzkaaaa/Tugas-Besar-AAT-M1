package m2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/problem3"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type combinedInbox struct {
	storage.IngestInbox
	items []storage.IngestMessage
}

func (s *combinedInbox) Put(_ context.Context, m storage.IngestMessage) error {
	for _, old := range s.items {
		if old.ID == m.ID {
			return nil
		}
	}
	s.items = append(s.items, m)
	return nil
}
func (s *combinedInbox) ListMessages(_ context.Context, source string, _ storage.IngestCursor, _ int) (storage.IngestPage, error) {
	p := storage.IngestPage{}
	for _, m := range s.items {
		if source == "" || source == m.Source {
			p.Items = append(p.Items, m)
		}
	}
	return p, nil
}

type combinedHazards struct{ storage.HazardStore }

func (combinedHazards) List(context.Context, storage.HazardQuery, storage.PageRequest) (storage.HazardPage, error) {
	return storage.HazardPage{Items: []*domain.HazardRecord{{HazardID: "fire-id", Source: domain.SourceSatellite, SourceRefID: "fire-ref", HazardType: domain.HazardTypeWildfire}}}, nil
}

func TestP3P5ShareInternalHTTPAndQuarantine(t *testing.T) {
	inbox := &combinedInbox{items: []storage.IngestMessage{{ID: "p3-item", Source: "PVMBG", State: storage.IngestParked}}}
	// No StreamStore: parking must use P3's durable inbox when available.
	backend := &storage.Backend{Inbox: inbox, Hazards: combinedHazards{}}
	r := NewRunner(Config{AuditToken: "integration-audit-token"}, backend)
	for _, source := range []string{domain.SourceSatellite, domain.SourceCoastal} {
		for range 2 {
			if err := r.park(context.Background(), source, []byte("private-raw-input"), "invalid_json", time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(inbox.items) != 3 {
		t.Fatalf("quarantine duplicated or lost messages: %d", len(inbox.items))
	}
	verifier := &auth.JWTVerifier{}
	srv := httptest.NewServer(problem3.Handler(backend, verifier, r.Config.AuditToken, r.AuditHandler(verifier)))
	defer srv.Close()
	for _, tc := range []struct {
		path  string
		count int
	}{
		{"/internal/audit/unprocessed", 3},
		{"/internal/audit/unprocessed?source=SATELIT", 1},
		{"/internal/audit/unprocessed?source=SENSOR_PANTAI", 1},
		{"/internal/audit/events?source=SATELIT", 1},
	} {
		req, _ := http.NewRequest("GET", srv.URL+tc.path, nil)
		req.Header.Set("Authorization", "Bearer "+r.Config.AuditToken)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Items []json.RawMessage `json:"items"`
		}
		err = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || len(body.Items) != tc.count {
			t.Fatalf("%s: status %d count %d error %v", tc.path, resp.StatusCode, len(body.Items), err)
		}
		for _, item := range body.Items {
			if strings.Contains(string(item), "private-raw-input") {
				t.Fatal("audit leaked raw input")
			}
		}
	}
	for _, path := range []string{"/internal/ops/ingest/" + inbox.items[1].ID + "/payload", "/internal/audit/checksum"} {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		// No credentials may read neither raw payload nor canonical audit.
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Fatalf("unprotected route %s: %d", path, resp.StatusCode)
		}
	}
}

func TestSharedAuditConfiguration(t *testing.T) {
	t.Setenv("SATELLITE_BASE_URL", "")
	t.Setenv("PANTAI_BASE_URL", "")
	t.Setenv("AUDIT_TOKEN", "legacy-audit-token")
	t.Setenv("AUDIT_API_TOKEN", "canonical-audit-token")
	t.Setenv("AGGREGATOR_INTERNAL_HTTP_PORT", "9092")
	t.Setenv("AGGREGATOR_M2_HTTP_ADDRESS", ":9092")
	c, err := LoadConfig()
	if err != nil || c.AuditToken != "canonical-audit-token" || c.HTTPAddress != ":9092" {
		t.Fatalf("shared config: %+v %v", c, err)
	}
	t.Setenv("AGGREGATOR_INTERNAL_HTTP_PORT", "9093")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("accepted conflicting listeners")
	}
}
