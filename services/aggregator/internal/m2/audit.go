package m2

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

// AuditHandler exposes the canonical store's indexed audit surface.
func (r *Runner) AuditHandler(verifier *auth.JWTVerifier) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /internal/audit/events", r.auditEvents)
	mux.HandleFunc("GET /internal/audit/checksum", r.auditChecksum)
	mux.HandleFunc("GET /internal/audit/location", r.auditLocation)
	mux.HandleFunc("GET /internal/audit/unprocessed", func(w http.ResponseWriter, q *http.Request) { r.auditParked(w, q, false) })
	mux.HandleFunc("GET /internal/ops/m2/unprocessed", func(w http.ResponseWriter, q *http.Request) { r.auditParked(w, q, true) })
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		auditError(w, 404, "not_found", "endpoint tidak ditemukan")
	})
	return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == "/healthz" {
			mux.ServeHTTP(w, q)
			return
		}
		token := strings.TrimPrefix(q.Header.Get("Authorization"), "Bearer ")
		if strings.HasPrefix(q.URL.Path, "/internal/ops/") {
			if verifier == nil {
				auditError(w, 401, "unauthorized", "token tidak valid")
				return
			}
			caller, err := verifier.VerifyToken(token)
			if err != nil {
				auditError(w, 401, "unauthorized", "token tidak valid")
				return
			}
			if caller.Role != "BNPB_INTERNAL" || !caller.HasScope("ingest:inspect") {
				auditError(w, 403, "forbidden", "BNPB Internal Ops with ingest:inspect required")
				return
			}
		} else if r.Config.AuditToken == "" || subtle.ConstantTimeCompare([]byte(token), []byte(r.Config.AuditToken)) != 1 {
			auditError(w, 401, "unauthorized", "token tidak valid")
			return
		}
		timeout := 5 * time.Second
		if q.URL.Path == "/internal/audit/checksum" {
			timeout = 20 * time.Second
		}
		ctx, cancel := context.WithTimeout(q.Context(), timeout)
		defer cancel()
		mux.ServeHTTP(w, q.WithContext(ctx))
	})
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func auditError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func auditSource(w http.ResponseWriter, q *http.Request) (string, bool) {
	s := q.URL.Query().Get("source")
	if s != "" && domain.SourceKeyFor(s) == "" {
		auditError(w, 400, "bad_request", "unknown source")
		return "", false
	}
	return s, true
}
func pageLimit(w http.ResponseWriter, q *http.Request) (int, bool) {
	v := q.URL.Query().Get("limit")
	if v == "" {
		return 100, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 1000 {
		auditError(w, 400, "bad_request", "limit must be 1..1000")
		return 0, false
	}
	return n, true
}

type auditCursor struct {
	Source, Since string
	At            time.Time
	ID            string
	Start         bool
}

func (r *Runner) auditEvents(w http.ResponseWriter, q *http.Request) {
	source, ok := auditSource(w, q)
	if !ok {
		return
	}
	limit, ok := pageLimit(w, q)
	if !ok {
		return
	}
	sinceText := q.URL.Query().Get("since")
	var since time.Time
	if sinceText != "" {
		var err error
		since, err = time.Parse(time.RFC3339Nano, sinceText)
		if err != nil {
			auditError(w, 400, "bad_request", "invalid since")
			return
		}
	}
	c := auditCursor{Source: source, Since: sinceText}
	if raw := q.URL.Query().Get("cursor"); raw != "" {
		b, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil || len(b) > 2048 || json.Unmarshal(b, &c) != nil || c.Source != source || c.Since != sinceText ||
			(c.ID == "" && !c.Start) || (c.Start && (c.ID != "" || !c.At.IsZero())) {
			auditError(w, 400, "bad_request", "invalid cursor")
			return
		}
	}
	if r.Store.Audit == nil {
		auditError(w, 503, "unavailable", "audit index unavailable")
		return
	}
	page, err := r.Store.Audit.Events(q.Context(), storage.AuditQuery{Source: source, Since: since, After: storage.AuditPosition{At: c.At, ID: c.ID}, Limit: limit})
	units := page.UnavailableUnits
	if err != nil && len(units) == 0 {
		units = append(units, r.Store.Name+"-canonical")
	}
	partial := len(units) > 0
	items := page.Items
	if items == nil {
		items = []*domain.HazardRecord{}
	}
	var next any
	if partial {
		if c.ID == "" {
			c.Start = true
		}
		b, _ := json.Marshal(c)
		next = base64.RawURLEncoding.EncodeToString(b)
	} else if page.More && len(items) > 0 {
		last := items[len(items)-1]
		b, _ := json.Marshal(auditCursor{Source: source, Since: sinceText, At: last.IngestedAt, ID: last.HazardID})
		next = base64.RawURLEncoding.EncodeToString(b)
	}
	if units == nil {
		units = []string{}
	}
	writeJSON(w, 200, map[string]any{"items": items, "next_cursor": next, "partial": partial, "unavailable_units": units})
}
func (r *Runner) auditChecksum(w http.ResponseWriter, q *http.Request) {
	source, ok := auditSource(w, q)
	if !ok {
		return
	}
	if r.Store.Audit == nil {
		auditError(w, 503, "unavailable", "audit index unavailable")
		return
	}
	hash := sha256.New()
	count := int64(0)
	units, err := r.Store.Audit.References(q.Context(), source, func(ref string) error {
		if count > 0 {
			_, _ = hash.Write([]byte{'\n'})
		}
		_, _ = hash.Write([]byte(ref))
		count++
		return nil
	})
	if err != nil && len(units) == 0 {
		units = append(units, r.Store.Name+"-canonical")
	}
	if units == nil {
		units = []string{}
	}
	writeJSON(w, 200, map[string]any{"source": source, "count": count, "checksum": hex.EncodeToString(hash.Sum(nil)), "algorithm": "sha256", "as_of": time.Now().UTC(), "partial": len(units) > 0, "unavailable_units": units})
}

func (r *Runner) auditLocation(w http.ResponseWriter, q *http.Request) {
	id := q.URL.Query().Get("hazard_id")
	if id == "" || len(id) > 256 {
		auditError(w, 400, "bad_request", "hazard_id is required")
		return
	}
	if r.Store.Placement == nil {
		auditError(w, 503, "unavailable", "placement metadata unavailable")
		return
	}
	result, err := r.Store.Placement.Locate(q.Context(), id)
	if err != nil && len(result.UnavailableUnits) == 0 {
		result.UnavailableUnits = []string{"placement"}
	}
	partial := len(result.UnavailableUnits) > 0
	if !result.Exists && !partial {
		auditError(w, 404, "not_found", "hazard_id tidak ditemukan")
		return
	}
	if result.Locations == nil {
		result.Locations = []storage.HazardLocation{}
	}
	if result.UnavailableUnits == nil {
		result.UnavailableUnits = []string{}
	}
	writeJSON(w, 200, map[string]any{"hazard_id": id, "locations": result.Locations, "partial": partial, "unavailable_units": result.UnavailableUnits})
}
func (r *Runner) auditParked(w http.ResponseWriter, q *http.Request, raw bool) {
	source, ok := auditSource(w, q)
	if !ok {
		return
	}
	limit, ok := pageLimit(w, q)
	if !ok {
		return
	}
	after := ""
	if c := q.URL.Query().Get("cursor"); c != "" {
		b, err := base64.RawURLEncoding.DecodeString(c)
		if err != nil {
			auditError(w, 400, "bad_request", "invalid cursor")
			return
		}
		after = string(b)
	}
	items := []storage.ParkedInput{}
	sources := []string{source}
	if source == "" {
		sources = []string{domain.SourceSatellite, domain.SourceCoastal}
	}
	for _, s := range sources {
		rows, err := r.Store.Streams.Parked(q.Context(), s, after, limit+1)
		if err != nil {
			auditError(w, 503, "unavailable", "input store unavailable")
			return
		}
		items = append(items, rows...)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].MessageID < items[j].MessageID })
	var next any
	if len(items) > limit {
		items = items[:limit]
		next = base64.RawURLEncoding.EncodeToString([]byte(items[len(items)-1].MessageID))
	}
	if !raw {
		for i := range items {
			items[i].Payload = nil
		}
	}
	writeJSON(w, 200, map[string]any{"items": items, "next_cursor": next})
}
