package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestLoggerAddsTraceCorrelation(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "aggregator")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	log.InfoContext(trace.ContextWithSpanContext(context.Background(), sc), "hazard saved", "hazard_id", "hz-1")
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatal(err)
	}
	if rec["trace_id"] != sc.TraceID().String() || rec["span_id"] != sc.SpanID().String() || rec["service.name"] != "aggregator" || rec["hazard_id"] != "hz-1" {
		t.Fatalf("record: %v", rec)
	}
	buf.Reset()
	log.Info("untraced")
	if bytes.Contains(buf.Bytes(), []byte("trace_id")) {
		t.Fatal("untraced record has a trace id")
	}
}
