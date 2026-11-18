package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// correlated adds the active span's trace_id/span_id to every record logged
// with a context, so logs join traces in the backend (Loki -> Tempo).
type correlated struct{ slog.Handler }

func (h correlated) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}
func (h correlated) WithAttrs(a []slog.Attr) slog.Handler { return correlated{h.Handler.WithAttrs(a)} }
func (h correlated) WithGroup(n string) slog.Handler      { return correlated{h.Handler.WithGroup(n)} }

// NewLogger returns a JSON logger whose records carry service.name and, when
// logged with a traced context, trace_id/span_id. LOG_LEVEL selects
// debug|info|warn|error (default info).
func NewLogger(w io.Writer, service string) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	return slog.New(correlated{slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})}).With("service.name", service)
}
