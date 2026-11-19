package storetest

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func testGetSummary(t *testing.T, b *storage.Backend) {
	if h, err := b.Hazards.GetSummary(ctx(t), "missing"); err != nil || h != nil {
		t.Fatal("missing summary", h, err)
	}
	h := hazard(domain.SourceSatellite, "get-summary", base)
	h.Attributes = map[string]any{"body": strings.Repeat("x", 3<<20)}
	recorder := &eventRecorder{}
	for _, withdrawn := range []any{nil, true, "true", false} {
		h.Attributes["withdrawn"] = withdrawn
		saved, _ := apply(t, b, recorder, h)
		summary, err := b.Hazards.GetSummary(ctx(t), saved.HazardID)
		if err != nil {
			t.Fatal(err)
		}
		if withdrawn == true {
			if summary != nil {
				t.Fatal("withdrawn summary exposed")
			}
		} else if !reflect.DeepEqual(summary, storage.HazardSummaryHeader(saved)) {
			t.Fatal("summary fields differ or contain details", summary)
		}
		full, err := b.Hazards.Get(ctx(t), saved.HazardID)
		if err != nil || full == nil || full.Attributes["body"] != h.Attributes["body"] || domain.Withdrawn(full) != (withdrawn == true) {
			t.Fatal("summary read changed full canonical content", err)
		}
	}
}
