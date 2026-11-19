package storetest

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func testOutboxPayloadBudgetAndFairness(t *testing.T, b *storage.Backend) {
	payload := bytes.Repeat([]byte("large"), 3*(1<<20)/5+1)[:3<<20]
	for i := range 5 {
		h := hazard(domain.SourceBMKG, fmt.Sprintf("bulk-%d", i), base.Add(time.Duration(i)*time.Second))
		if _, _, err := b.Hazards.ApplyRevision(ctx(t), h, func(saved *domain.HazardRecord) (storage.OutboxEvent, error) {
			return storage.OutboxEvent{EventID: fmt.Sprintf("bulk-%s-r%d", saved.HazardID, saved.Revision), HazardID: saved.HazardID, Source: saved.Source, Payload: payload}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	coast := hazard(domain.SourceCoastal, "priority", base)
	coast.HazardType = domain.HazardTypeCoastal
	if _, _, err := b.Hazards.ApplyRevision(ctx(t), coast, func(saved *domain.HazardRecord) (storage.OutboxEvent, error) {
		return storage.OutboxEvent{EventID: "priority-" + saved.HazardID, HazardID: saved.HazardID, Source: saved.Source, Payload: []byte("coastal")}, nil
	}); err != nil {
		t.Fatal(err)
	}
	first := claim(t, b, 50, time.Minute)
	// The 8MiB aggregate target permits one complete boundary record. Never
	// truncate an event, starve a large head, or lease unreturned candidates.
	if len(first) != 4 {
		t.Fatalf("first batch has %d events, want coastal plus three 3MiB records", len(first))
	}
	if first[0].Source != domain.SourceCoastal {
		t.Fatal("large regular backlog hid the coastal warning")
	}
	bytesRead := 0
	seen := map[string]bool{}
	for _, ev := range first {
		seen[ev.EventID] = true
		bytesRead += len(ev.Payload)
		if ev.Source != domain.SourceCoastal && !bytes.Equal(ev.Payload, payload) {
			t.Fatal("outbox payload was truncated")
		}
	}
	if bytesRead > 8<<20+len(payload) {
		t.Fatal("payload budget exceeded by more than one complete record", bytesRead)
	}
	second := claim(t, b, 50, time.Minute)
	if len(second) != 2 {
		t.Fatalf("unreturned candidates were leased or lost: %d", len(second))
	}
	for _, ev := range second {
		if seen[ev.EventID] || !bytes.Equal(ev.Payload, payload) {
			t.Fatal("duplicate or altered remainder")
		}
		seen[ev.EventID] = true
	}
	if len(claim(t, b, 50, time.Minute)) != 0 {
		t.Fatal("leased bulk events reappeared")
	}
	for id := range seen {
		if err := b.Outbox.Ack(ctx(t), id); err != nil {
			t.Fatal(err)
		}
	}
	if len(claim(t, b, 50, time.Minute)) != 0 {
		t.Fatal("acknowledged bulk events reappeared")
	}
}

// Both classes can have records larger than their reserved capacity. Advancing
// only coastal work until its backlog empties would starve regular sources.
func testOutboxLargePriorityClasses(t *testing.T, b *storage.Backend) {
	payload := bytes.Repeat([]byte("x"), 3<<20)
	for _, source := range []string{domain.SourceCoastal, domain.SourceSatellite} {
		for i := range 3 {
			h := hazard(source, fmt.Sprintf("large-priority-%s-%d", source, i), base.Add(time.Duration(i)*time.Second))
			if source == domain.SourceCoastal {
				h.HazardType = domain.HazardTypeCoastal
			}
			if _, _, err := b.Hazards.ApplyRevision(ctx(t), h, func(saved *domain.HazardRecord) (storage.OutboxEvent, error) {
				return storage.OutboxEvent{EventID: "priority-" + saved.HazardID, HazardID: saved.HazardID, Source: saved.Source, Payload: payload}, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	seen := map[string]bool{}
	for batch, wantPerClass := range []int{2, 1} {
		claimed := claim(t, b, 50, time.Minute)
		counts := map[string]int{}
		for _, ev := range claimed {
			if seen[ev.EventID] || !bytes.Equal(ev.Payload, payload) {
				t.Fatal("duplicate or truncated mixed-priority event")
			}
			seen[ev.EventID] = true
			counts[ev.Source]++
		}
		if len(claimed) != 2*wantPerClass || counts[domain.SourceCoastal] != wantPerClass || counts[domain.SourceSatellite] != wantPerClass {
			t.Fatalf("batch %d starved a class or leased unreturned work: %v", batch, counts)
		}
	}
	if len(claim(t, b, 50, time.Minute)) != 0 {
		t.Fatal("mixed-priority leases did not hold")
	}
}
