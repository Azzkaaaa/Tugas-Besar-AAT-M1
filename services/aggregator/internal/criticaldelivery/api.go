package criticaldelivery

import (
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type API struct {
	Journal    storage.CriticalJournal
	Token      string
	ReceiverID string
	PublicKey  ed25519.PublicKey
}

func (a API) Handler(fallback http.Handler) http.Handler {
	if fallback == nil {
		fallback = http.NotFoundHandler()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/critical/pending", func(w http.ResponseWriter, r *http.Request) { a.list(w, r, true) })
	mux.HandleFunc("GET /internal/critical/events", func(w http.ResponseWriter, r *http.Request) { a.list(w, r, false) })
	mux.HandleFunc("POST /internal/critical/receipts", a.receipt)
	mux.HandleFunc("GET /internal/critical/version", func(w http.ResponseWriter, r *http.Request) {
		versions, err := a.Journal.Version(r.Context())
		if err != nil {
			respond(w, 503, map[string]string{"error": "snapshot unavailable"})
			return
		}
		respond(w, 200, struct {
			Versions []int64 `json:"versions"`
		}{versions})
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/internal/critical/") {
			fallback.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || len(a.Token) < 16 || subtle.ConstantTimeCompare([]byte(token), []byte(a.Token)) != 1 {
			respond(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		if a.Journal == nil || len(a.PublicKey) != ed25519.PublicKeySize || a.ReceiverID == "" {
			respond(w, 503, map[string]string{"error": "critical delivery unavailable"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
	return otelhttp.NewHandler(handler, "aggregator.critical", otelhttp.WithFilter(func(r *http.Request) bool { return strings.HasPrefix(r.URL.Path, "/internal/critical/") }))
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (a API) list(w http.ResponseWriter, r *http.Request, pending bool) {
	cursor := storage.CriticalCursor{}
	if value := r.URL.Query().Get("cursor"); value != "" {
		raw, err := base64.RawURLEncoding.DecodeString(value)
		if err != nil || len(raw) > 512 || json.Unmarshal(raw, &cursor) != nil || cursor.Shard < 0 || cursor.Shard >= 16 || len(cursor.After) > 128 {
			respond(w, 400, map[string]string{"error": "invalid cursor"})
			return
		}
	}
	limit := 32
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			respond(w, 400, map[string]string{"error": "invalid limit"})
			return
		}
		limit = n
	}
	var page storage.CriticalPage
	var err error
	var versions []int64
	if value := r.URL.Query().Get("versions"); value != "" {
		raw, decodeErr := base64.RawURLEncoding.DecodeString(value)
		if decodeErr != nil || len(raw) > 1024 || json.Unmarshal(raw, &versions) != nil {
			respond(w, 400, map[string]string{"error": "invalid snapshot"})
			return
		}
		current, versionErr := a.Journal.Version(r.Context())
		if versionErr != nil {
			respond(w, 503, map[string]string{"error": "snapshot unavailable"})
			return
		}
		if len(current) != len(versions) {
			respond(w, 400, map[string]string{"error": "invalid snapshot"})
			return
		}
		for i, v := range versions {
			if v < 0 || v > current[i] {
				respond(w, 400, map[string]string{"error": "invalid snapshot"})
				return
			}
		}
	}
	if pending {
		page, err = a.Journal.Pending(r.Context(), cursor, limit)
	} else if versions != nil {
		page, err = a.Journal.ListSnapshot(r.Context(), cursor, limit, versions)
	} else {
		page, err = a.Journal.List(r.Context(), cursor, limit)
	}
	if err != nil {
		respond(w, 503, map[string]string{"error": "critical storage unavailable"})
		return
	}
	next := ""
	if page.Next != nil {
		raw, _ := json.Marshal(page.Next)
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	respond(w, 200, Page{Events: page.Events, Next: next, Partial: len(page.UnavailableUnits) > 0, UnavailableUnits: page.UnavailableUnits})
}

type Page struct {
	Events           []storage.CriticalEvent `json:"events"`
	Next             string                  `json:"next_cursor,omitempty"`
	Partial          bool                    `json:"partial"`
	UnavailableUnits []string                `json:"unavailable_units,omitempty"`
}

func (a API) receipt(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var receipt storage.CriticalReceipt
	if err := decoder.Decode(&receipt); err != nil {
		respond(w, 400, map[string]string{"error": "invalid receipt"})
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF || !receipt.RecordedAt.IsZero() {
		respond(w, 400, map[string]string{"error": "invalid receipt"})
		return
	}
	if err := VerifyReceipt(a.PublicKey, a.ReceiverID, receipt); err != nil {
		respond(w, 403, map[string]string{"error": "receiver proof required"})
		return
	}
	e, err := a.Journal.Get(r.Context(), receipt.HazardID, receipt.EventID)
	if errors.Is(err, storage.ErrCriticalNotFound) {
		respond(w, 404, map[string]string{"error": "unknown critical event"})
		return
	}
	if err != nil {
		respond(w, 503, map[string]string{"error": "critical storage unavailable"})
		return
	}
	if e.PayloadSHA256 != receipt.PayloadSHA256 {
		respond(w, 409, map[string]string{"error": "receipt payload mismatch"})
		return
	}
	if err := a.Journal.RecordReceipt(r.Context(), receipt); err != nil {
		respond(w, 503, map[string]string{"error": "receipt not durable; retry"})
		return
	}
	respond(w, 200, map[string]string{"status": "recorded"})
}
