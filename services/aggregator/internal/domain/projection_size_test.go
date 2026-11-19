package domain

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestFullProjectionSizeMatchesActualWire(t *testing.T) {
	numbers := make([]any, 100000)
	for i := range numbers {
		numbers[i] = float64(i)
	}
	cases := []map[string]any{
		nil, {}, {"null": nil, "false": false, "true": true, "zero": float64(0), "nan": math.NaN()},
		{"nested": map[string]any{"values": []any{nil, true, float64(3), "UTF-8 🌋"}}},
		{"bytes": []byte("binary"), "integer": int64(123), "number": json.Number("3.14159")},
		{"long": strings.Repeat("x", 16384), "numbers": numbers},
		{"unsupported": []string{"typed slice"}}, {"invalid": "\xff"}, {"\xff": "invalid key"},
		{"invalid-number": json.Number("bad")},
	}
	for i, attrs := range cases {
		h := &HazardRecord{HazardID: "hz-test", Source: SourceSatellite, SourceRefID: "ref", HazardType: HazardTypeWildfire,
			Severity: SeverityAwas, AreaName: "Area", OccurredAt: time.Unix(1, 123000000), IngestedAt: time.Unix(2, 0), Attributes: attrs}
		projection, err := h.ToProjection(commonv1.View_VIEW_FULL)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := FullProjectionSize(h), proto.Size(projection); got != want {
			t.Fatalf("case %d: predicted=%d actual=%d", i, got, want)
		}
	}
}
