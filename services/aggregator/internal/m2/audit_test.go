package m2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type indexedAuditFixture struct {
	queries []storage.AuditQuery
	missing bool
}

func (s *indexedAuditFixture) Events(_ context.Context, q storage.AuditQuery) (storage.AuditPage, error) {
	s.queries = append(s.queries, q)
	page := storage.AuditPage{Items: []*domain.HazardRecord{{HazardID: "h", Source: "BMKG", SourceRefID: "r", IngestedAt: time.Date(2026, 11, 18, 1, 0, 0, 0, time.UTC)}}, More: true}
	if s.missing {
		page.UnavailableUnits = []string{"audit-shard-03"}
	}
	return page, nil
}
func (*indexedAuditFixture) References(_ context.Context, _ string, visit func(string) error) ([]string, error) {
	for _, ref := range []string{"A", "a", "z", "ä"} {
		if err := visit(ref); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
func (*indexedAuditFixture) Locate(_ context.Context, id string) (storage.PlacementResult, error) {
	return storage.PlacementResult{Exists: id == "known", Locations: []storage.HazardLocation{{Node: "node-a", Unit: "store-a", Role: "primary"}}}, nil
}

func TestAuditUsesIndexedCursorAndNeverAdvancesPartialInterval(t *testing.T) {
	f := &indexedAuditFixture{missing: true}
	runner := NewRunner(Config{AuditToken: "local-audit-fixture"}, &storage.Backend{Audit: f, Placement: f})
	server := httptest.NewServer(runner.AuditHandler(nil))
	defer server.Close()
	request := func(path string) (int, map[string]any) {
		t.Helper()
		q, _ := http.NewRequest("GET", server.URL+path, nil)
		q.Header.Set("Authorization", "Bearer local-audit-fixture")
		r, err := server.Client().Do(q)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return r.StatusCode, body
	}
	code, first := request("/internal/audit/events?source=BMKG&limit=1&since=2026-11-18T00:00:00Z")
	if code != 200 || first["partial"] != true || !reflect.DeepEqual(first["unavailable_units"], []any{"audit-shard-03"}) {
		t.Fatal(first)
	}
	cursor := url.QueryEscape(first["next_cursor"].(string))
	_, _ = request("/internal/audit/events?source=BMKG&limit=1&since=2026-11-18T00:00:00Z&cursor=" + cursor)
	if f.queries[1].After.ID != "" {
		t.Fatal("partial cursor skipped missing interval")
	}
	f.missing = false
	_, complete := request("/internal/audit/events?source=BMKG&limit=1&since=2026-11-18T00:00:00Z&cursor=" + cursor)
	_, _ = request("/internal/audit/events?source=BMKG&limit=1&since=2026-11-18T00:00:00Z&cursor=" + url.QueryEscape(complete["next_cursor"].(string)))
	if f.queries[3].After.ID != "h" {
		t.Fatal("complete cursor failed to advance")
	}
	if code, _ := request("/internal/audit/events?source=PVMBG&cursor=" + cursor); code != 400 {
		t.Fatal("cursor accepted for another query")
	}
	_, checksum := request("/internal/audit/checksum")
	want := sha256.Sum256([]byte("A\na\nz\nä"))
	if checksum["checksum"] != hex.EncodeToString(want[:]) || checksum["count"] != float64(4) {
		t.Fatal(checksum)
	}
	for _, tc := range []struct {
		path string
		code int
	}{{"/internal/audit/location", 400}, {"/internal/audit/location?hazard_id=unknown", 404}, {"/internal/audit/location?hazard_id=known", 200}, {"/internal/audit/missing", 404}} {
		if code, _ := request(tc.path); code != tc.code {
			t.Fatalf("%s: %d", tc.path, code)
		}
	}
}
