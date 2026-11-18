package telemetry

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
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

// Fatal logs at error level and exits, replacing log.Fatal in services.
func Fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

// fanout sends each record to every handler (stdout JSON and OTLP).
type fanout []slog.Handler

func (f fanout) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range f {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}
func (f fanout) Handle(ctx context.Context, r slog.Record) error {
	var first error
	for _, h := range f {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}
func (f fanout) WithAttrs(a []slog.Attr) slog.Handler {
	out := make(fanout, len(f))
	for i, h := range f {
		out[i] = h.WithAttrs(a)
	}
	return out
}
func (f fanout) WithGroup(n string) slog.Handler {
	out := make(fanout, len(f))
	for i, h := range f {
		out[i] = h.WithGroup(n)
	}
	return out
}

// ExportLogs keeps the current default logger's output and additionally
// exports every record through lp.
func ExportLogs(lp *sdklog.LoggerProvider, service string) {
	current := slog.Default().Handler()
	// The built-in default handler writes through package log, which SetDefault
	// redirects back into slog: wrapping it would deadlock. Use JSON instead.
	if fmt.Sprintf("%T", current) == "*slog.defaultHandler" {
		current = slog.NewJSONHandler(os.Stderr, nil)
	}
	slog.SetDefault(slog.New(fanout{current, otelslog.NewHandler(service, otelslog.WithLoggerProvider(lp))}))
}

func newLogExporter(ctx context.Context, endpoint string) (sdklog.Exporter, error) {
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return otlploghttp.New(ctx, otlploghttp.WithEndpointURL(strings.TrimRight(endpoint, "/")+"/v1/logs"))
	}
	return otlploghttp.New(ctx, otlploghttp.WithEndpoint(endpoint), otlploghttp.WithInsecure())
}
