package storetest

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func testListPayloadBudgetAndSummary(t *testing.T, b *storage.Backend) {
	body := strings.Repeat("large-fire-", 3*(1<<20)/11+1)[:3<<20]
	rec := &eventRecorder{}
	var want []*domain.HazardRecord
	for i := range 5 {
		h := hazard(domain.SourceSatellite, fmt.Sprintf("page-%d", i), base.Add(time.Duration(i/2)*24*time.Hour))
		h.HazardType = domain.HazardTypeWildfire
		h.Attributes = map[string]any{"body": body}
		saved, _ := apply(t, b, rec, h)
		want = append(want, saved)
	}
	// Withdrawal is a JSON boolean, not the string "true".
	withdrawn := hazard(domain.SourceSatellite, "withdrawn", base.Add(4*24*time.Hour))
	withdrawn.Attributes = map[string]any{"withdrawn": true}
	apply(t, b, rec, withdrawn)
	nonBoolean := hazard(domain.SourceSatellite, "string-true", base.Add(-24*time.Hour))
	nonBoolean.Attributes = map[string]any{"withdrawn": "true"}
	saved, _ := apply(t, b, rec, nonBoolean)
	want = append(want, saved)
	sort.Slice(want, func(i, j int) bool {
		if !want[i].OccurredAt.Equal(want[j].OccurredAt) {
			return want[i].OccurredAt.After(want[j].OccurredAt)
		}
		return want[i].HazardID < want[j].HazardID
	})
	var got []*domain.HazardRecord
	token := ""
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("payload-limited cursor did not advance")
		}
		page, err := b.Hazards.List(ctx(t), storage.HazardQuery{}, storage.PageRequest{Size: 100, Token: token})
		if err != nil || page.Partial {
			t.Fatalf("full list: %v %+v", err, page)
		}
		if len(page.Items) == 0 {
			t.Fatal("byte budget made a false terminal/empty page")
		}
		if pages == 0 && (len(page.Items) != 2 || page.NextToken == "") {
			t.Fatalf("first page was not byte bounded: %d token=%q", len(page.Items), page.NextToken)
		}
		for _, h := range page.Items {
			if h.SourceRefID != "string-true" && h.Attributes["body"] != body {
				t.Fatal("full payload lost or truncated", h.HazardID)
			}
		}
		got = append(got, page.Items...)
		if page.NextToken == "" {
			break
		}
		if page.NextToken == token {
			t.Fatal("complete page did not advance")
		}
		token = page.NextToken
	}
	assertIDs(t, got, want)
	summary, err := b.Hazards.List(ctx(t), storage.HazardQuery{SummaryOnly: true}, storage.PageRequest{Size: 100})
	if err != nil || summary.Partial || summary.NextToken != "" {
		t.Fatal(err, summary.Partial, summary.NextToken)
	}
	assertIDs(t, summary.Items, want)
	for _, h := range summary.Items {
		if h.Attributes != nil || h.SourceRefID != "" || h.Latitude != 0 || h.Longitude != 0 {
			t.Fatal("summary retained detail", h.HazardID)
		}
	}
}
