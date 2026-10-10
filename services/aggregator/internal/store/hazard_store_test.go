package store

import (
	"testing"
	"time"
)

func TestCursorRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hazardID := "hz-test-12345"

	encoded := encodeCursor(now, hazardID)
	if encoded == "" {
		t.Fatal("expected non-empty encoded cursor")
	}

	decoded, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding cursor: %v", err)
	}

	if decoded.HazardID != hazardID {
		t.Errorf("expected hazard ID %q, got %q", hazardID, decoded.HazardID)
	}

	if !decoded.OccurredAt.Equal(now) {
		t.Errorf("expected occurredAt %v, got %v", now, decoded.OccurredAt)
	}
}

func TestDecodeCursor_Empty(t *testing.T) {
	c, err := decodeCursor("")
	if err != nil {
		t.Fatalf("expected nil error on empty cursor, got: %v", err)
	}
	if c != nil {
		t.Fatalf("expected nil cursor on empty string, got: %+v", c)
	}
}
