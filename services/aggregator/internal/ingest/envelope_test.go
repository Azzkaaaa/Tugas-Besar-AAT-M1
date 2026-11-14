package ingest

import (
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

func TestBuildPublishedEnvelopeNeedsRevision(t *testing.T) {
	h := &domain.HazardRecord{HazardID: "hz-1", Source: domain.SourceBMKG, Attributes: map[string]any{}}
	if _, _, err := BuildPublishedEnvelope(h, "c"); err == nil {
		t.Fatal("record without a committed revision accepted")
	}
	h.Revision = 3
	id, _, err := BuildPublishedEnvelope(h, "c")
	if err != nil || id != domain.EventID("hz-1", 3) {
		t.Fatalf("envelope ID %q, %v", id, err)
	}
}
