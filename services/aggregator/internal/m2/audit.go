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

// AuditHandler exposes the P4/P5 adapter's audit surface through Aggregator.
// It deliberately does not invent node placement for /location (owned by P1).
func (r *Runner) AuditHandler(verifier *auth.JWTVerifier) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /internal/audit/events", r.auditEvents)
	mux.HandleFunc("GET /internal/audit/checksum", r.auditChecksum)
	mux.HandleFunc("GET /internal/audit/unprocessed", func(w http.ResponseWriter, q *http.Request) { r.auditParked(w, q, false) })
	mux.HandleFunc("GET /internal/ops/m2/unprocessed", func(w http.ResponseWriter, q *http.Request) { r.auditParked(w, q, true) })
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
		ctx, cancel := context.WithTimeout(q.Context(), 5*time.Second)
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
func (r *Runner) all(ctx context.Context, source string) ([]*domain.HazardRecord, bool) {
	out := []*domain.HazardRecord{}
	cursor := ""
	for {
		page, err := r.Store.Hazards.List(ctx, storage.HazardQuery{Source: source}, storage.PageRequest{Size: 1000, Token: cursor})
		if err != nil {
			return out, true
		}
		for _, h := range page.Items {
			if !domain.Withdrawn(h) {
				out = append(out, h)
			}
		}
		if page.Partial {
			return out, true
		}
		if page.NextToken == "" {
			break
		}
		if page.NextToken == cursor {
			return out, true
		}
		cursor = page.NextToken
	}
	return out, false
}
func (r *Runner) units(partial bool) []string {
	if partial {
		return []string{r.Store.Name + "-canonical"}
	}
	return []string{}
}

type auditCursor struct {
	Source, Since string
	At            time.Time
	ID            string
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
		if err != nil || json.Unmarshal(b, &c) != nil || c.Source != source || c.Since != sinceText || c.ID == "" {
			auditError(w, 400, "bad_request", "invalid cursor")
			return
		}
	}
	all, partial := r.all(q.Context(), source)
	sort.Slice(all, func(i, j int) bool {
		if all[i].IngestedAt.Equal(all[j].IngestedAt) {
			return all[i].HazardID < all[j].HazardID
		}
		return all[i].IngestedAt.Before(all[j].IngestedAt)
	})
	items := []*domain.HazardRecord{}
	for _, h := range all {
		if h.IngestedAt.Before(since) {
			continue
		}
		if c.ID != "" && (h.IngestedAt.Before(c.At) || (h.IngestedAt.Equal(c.At) && h.HazardID <= c.ID)) {
			continue
		}
		items = append(items, h)
		if len(items) > limit {
			break
		}
	}
	var next any
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		b, _ := json.Marshal(auditCursor{Source: source, Since: sinceText, At: last.IngestedAt, ID: last.HazardID})
		next = base64.RawURLEncoding.EncodeToString(b)
	}
	writeJSON(w, 200, map[string]any{"items": items, "next_cursor": next, "partial": partial, "unavailable_units": r.units(partial)})
}
func (r *Runner) auditChecksum(w http.ResponseWriter, q *http.Request) {
	source, ok := auditSource(w, q)
	if !ok {
		return
	}
	items, partial := r.all(q.Context(), source)
	refs := make([]string, 0, len(items))
	for _, h := range items {
		refs = append(refs, h.SourceRefID)
	}
	sort.Strings(refs)
	sum := sha256.Sum256([]byte(strings.Join(refs, "\n")))
	writeJSON(w, 200, map[string]any{"source": source, "count": len(refs), "checksum": hex.EncodeToString(sum[:]), "algorithm": "sha256", "as_of": time.Now().UTC(), "partial": partial, "unavailable_units": r.units(partial)})
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
