package store

import "testing"

func TestTraceContextEncoding(t *testing.T) {
	raw, err := encodeTraceContext(map[string]string{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"})
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeTraceContext(raw)["traceparent"]; got != "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" {
		t.Fatalf("round trip lost traceparent: %q", got)
	}
	// No trace is stored as SQL NULL.
	if raw, _ := encodeTraceContext(map[string]string{}); raw != nil {
		t.Fatalf("empty carrier encoded as %q, want nil", raw)
	}
	// Malformed rows publish without a parent instead of failing.
	if got := decodeTraceContext([]byte("{not json")); got != nil {
		t.Fatalf("malformed trace context decoded as %+v, want nil", got)
	}
}
