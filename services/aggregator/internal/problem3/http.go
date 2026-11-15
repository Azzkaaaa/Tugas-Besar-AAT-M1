package problem3

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type api struct {
	store      *storage.Backend
	verifier   *auth.JWTVerifier
	auditToken string
}

func StartHTTP(ctx context.Context, port string, store *storage.Backend, verifier *auth.JWTVerifier, auditToken string) error {
	handler := (&api{store: store, verifier: verifier, auditToken: auditToken}).routes()
	server := &http.Server{Addr: ":" + port, Handler: otelhttp.NewHandler(handler, "aggregator.problem3"),
		ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second}
	stop := context.AfterFunc(ctx, func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	})
	defer stop()
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (a *api) routes() http.Handler {
	routes := http.NewServeMux()
	routes.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	if a.store.Inbox != nil && a.verifier != nil {
		routes.HandleFunc("GET /internal/audit/unprocessed", a.listAudit)
		routes.HandleFunc("GET /internal/ops/ingest", a.ops("ingest:inspect", a.list))
		routes.HandleFunc("GET /internal/ops/ingest/{id}", a.ops("ingest:inspect", a.details))
		routes.HandleFunc("GET /internal/ops/ingest/{id}/payload", a.ops("ingest:inspect", a.payload))
		routes.HandleFunc("POST /internal/ops/ingest/{id}/replay", a.ops("ingest:replay", a.replay))
		routes.HandleFunc("GET /internal/ops/field-reports/{id}", a.ops("field-reports:read", a.getReport))
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		id := r.Header.Get("X-Correlation-ID")
		if len(id) == 0 || len(id) > 128 || strings.ContainsAny(id, "\r\n") {
			id = uuid.NewString()
		}
		w.Header().Set("X-Correlation-ID", id)
		routes.ServeHTTP(w, r)
	})
}

func bearer(r *http.Request) string {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if ok && strings.EqualFold(scheme, "Bearer") {
		return token
	}
	return ""
}

func (a *api) ops(scope string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := bearer(r)
		if token == "" || (a.auditToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(a.auditToken)) == 1) {
			failure(w, 401, "unauthorized", "operator bearer token required")
			return
		}
		caller, err := a.verifier.VerifyToken(token)
		switch {
		case errors.Is(err, auth.ErrAuthUnavailable):
			unavailable(w)
		case err != nil:
			failure(w, 401, "unauthorized", "operator bearer token required")
		case caller.Role != "BNPB_INTERNAL" || !caller.HasScope(scope):
			failure(w, 403, "forbidden", "insufficient operator permissions")
		default:
			next(w, r)
		}
	}
}

func (a *api) listAudit(w http.ResponseWriter, r *http.Request) {
	if a.auditToken == "" || subtle.ConstantTimeCompare([]byte(bearer(r)), []byte(a.auditToken)) != 1 {
		failure(w, 401, "unauthorized", "audit bearer token required")
		return
	}
	a.list(w, r)
}

type unprocessed struct {
	storage.IngestMessage
	Retryable bool `json:"retryable"`
}

type cursorToken struct {
	storage.IngestCursor
	Source string `json:"source"`
}

func (a *api) list(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	source := query.Get("source")
	switch source {
	case "", "PVMBG", "BMKG", "SATELIT", "SENSOR_PANTAI":
	default:
		failure(w, 400, "bad_request", "unknown source")
		return
	}
	limit := 100
	if value := query.Get("limit"); value != "" {
		var err error
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 1000 {
			failure(w, 400, "bad_request", "limit must be between 1 and 1000")
			return
		}
	}
	var cursor cursorToken
	if token := query.Get("cursor"); token != "" {
		data, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil || len(data) > 256 || json.Unmarshal(data, &cursor) != nil || cursor.Source != source ||
			cursor.Shard < 0 || cursor.Shard >= storage.IngestShardCount || !validMessageID(cursor.After) {
			failure(w, 400, "bad_request", "invalid cursor")
			return
		}
	}
	page, err := a.store.Inbox.ListMessages(r.Context(), source, cursor.IngestCursor, limit)
	if err != nil {
		unavailable(w)
		return
	}
	items := make([]unprocessed, 0, len(page.Items))
	for _, m := range page.Items {
		m.ReceivedAt = m.ReceivedAt.UTC()
		if m.State == storage.IngestPending {
			m.State, m.Reason = storage.IngestRetrying, "pending_processing"
		}
		items = append(items, unprocessed{IngestMessage: m, Retryable: m.State == storage.IngestRetrying})
	}
	var next *string
	if page.Next != nil {
		data, _ := json.Marshal(cursorToken{IngestCursor: *page.Next, Source: source})
		token := base64.RawURLEncoding.EncodeToString(data)
		next = &token
	}
	respond(w, 200, struct {
		Items       []unprocessed `json:"items"`
		Next        *string       `json:"next_cursor"`
		Partial     bool          `json:"partial,omitempty"`
		Unavailable []string      `json:"unavailable_units,omitempty"`
	}{items, next, len(page.UnavailableUnits) > 0, page.UnavailableUnits})
}

func validMessageID(id string) bool {
	if len(id) != 68 || !strings.HasPrefix(id, "msg-") {
		return false
	}
	_, err := hex.DecodeString(id[4:])
	return err == nil
}

func (a *api) message(w http.ResponseWriter, r *http.Request) *storage.IngestMessage {
	id := r.PathValue("id")
	if !validMessageID(id) {
		failure(w, 404, "not_found", "message not found")
		return nil
	}
	m, err := a.store.Inbox.GetMessage(r.Context(), id)
	if err != nil {
		unavailable(w)
		return nil
	} else if m == nil {
		failure(w, 404, "not_found", "message not found")
	}
	return m
}

func (a *api) payload(w http.ResponseWriter, r *http.Request) {
	m := a.message(w, r)
	if m == nil {
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="`+m.ID+`.bin"`)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if err := a.store.Inbox.WritePayload(r.Context(), *m, w); err != nil {
		slog.WarnContext(r.Context(), "inbox payload transfer failed", "message_id", m.ID, "correlation_id", w.Header().Get("X-Correlation-ID"))
		panic(http.ErrAbortHandler) // signal an interrupted transfer to the downloader
	}
}

func (a *api) details(w http.ResponseWriter, r *http.Request) {
	m := a.message(w, r)
	if m == nil {
		return
	}
	m.ReceivedAt = m.ReceivedAt.UTC()
	respond(w, 200, struct {
		storage.IngestMessage
		SourceRefID   string `json:"source_ref_id,omitempty"`
		SchemaVersion string `json:"schema_version,omitempty"`
		CorrelationID string `json:"correlation_id"`
	}{*m, m.SourceRefID, m.SchemaVersion, m.CorrelationID})
}

func (a *api) replay(w http.ResponseWriter, r *http.Request) {
	m := a.message(w, r)
	if m == nil {
		return
	}
	if m.State == storage.IngestParked {
		if m.SourceRefID == "" || !strings.HasPrefix(m.Reason, "schema_incompatible:") {
			failure(w, 400, "bad_request", "message needs a byte-level repair before replay")
			return
		}
		if _, err := a.store.Inbox.ChangeState(r.Context(), *m, storage.IngestRetrying, "operator_replay", m.Attempts, time.Time{}); err != nil {
			unavailable(w)
			return
		}
	}
	slog.InfoContext(r.Context(), "ingest replay requested", "message_id", m.ID, "correlation_id", w.Header().Get("X-Correlation-ID"))
	respond(w, 202, map[string]string{"message_id": m.ID})
}

func (a *api) getReport(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(strings.TrimPrefix(id, "fr-")); !strings.HasPrefix(id, "fr-") || err != nil {
		failure(w, 404, "not_found", "report not found")
		return
	}
	report, err := a.store.Reports.GetReport(r.Context(), id)
	switch {
	case err != nil:
		unavailable(w)
	case report == nil || report.ProcessedAt.IsZero():
		failure(w, 404, "not_found", "processed report not found")
	default:
		respond(w, 200, report)
	}
}

func respond(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func failure(w http.ResponseWriter, status int, code, message string) {
	respond(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func unavailable(w http.ResponseWriter) {
	failure(w, 503, "unavailable", "storage or authorization unavailable")
}
