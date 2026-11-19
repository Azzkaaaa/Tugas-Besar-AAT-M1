package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

func TestOutboxHydrationStopsBeforeReadingMoreBodies(t *testing.T) {
	candidates := make([]*ClaimedEvent, 10)
	for i := range candidates {
		candidates[i] = &ClaimedEvent{}
	}
	loads := 0
	payload := make([]byte, 2<<20)
	selected, err := LoadOutboxPayloads(context.Background(), candidates, func(ev *ClaimedEvent) (bool, error) { loads++; ev.Payload = payload; return true, nil })
	if err != nil || loads != 4 || len(selected) != 4 {
		t.Fatal("unbounded payload reads", loads, len(selected), err)
	}
	for _, ev := range candidates[4:] {
		if ev.Payload != nil {
			t.Fatal("unreturned candidate hydrated")
		}
	}
}

func TestOutboxHydrationPreservesLargeHeadAndPriorityPrefix(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		candidates := []*ClaimedEvent{{OutboxEvent: OutboxEvent{EventID: "large"}}, {OutboxEvent: OutboxEvent{EventID: "next"}}}
		if prefix {
			candidates = append([]*ClaimedEvent{{OutboxEvent: OutboxEvent{EventID: "coastal", Source: domain.SourceCoastal}}}, candidates...)
		}
		large := make([]byte, OutboxPayloadTargetBytes+1)
		loads := 0
		selected, err := LoadOutboxPayloads(context.Background(), candidates, func(ev *ClaimedEvent) (bool, error) {
			loads++
			if ev.EventID == "coastal" {
				ev.Payload = []byte("warning")
			} else {
				ev.Payload = large
			}
			return true, nil
		})
		want := 1
		if prefix {
			want = 2
		}
		if err != nil || len(selected) != want || loads != want || len(selected[want-1].Payload) != len(large) {
			t.Fatal("large event starved or truncated", len(selected), loads, err)
		}
	}
}

func TestOutboxByteBudgetReservesBothLargePriorityClasses(t *testing.T) {
	large := make([]byte, OutboxPayloadTargetBytes+1)
	var candidates []*ClaimedEvent
	for range 5 {
		candidates = append(candidates, &ClaimedEvent{OutboxEvent: OutboxEvent{Source: domain.SourceCoastal}})
	}
	candidates = append(candidates, &ClaimedEvent{OutboxEvent: OutboxEvent{Source: domain.SourceSatellite}})
	loads := 0
	selected, err := LoadOutboxPayloads(context.Background(), candidates, func(ev *ClaimedEvent) (bool, error) { loads++; ev.Payload = large; return true, nil })
	if err != nil || len(selected) != 2 || loads != 2 || selected[0].Source != domain.SourceCoastal || selected[1].Source != domain.SourceSatellite {
		t.Fatal("byte budget starved a priority class", len(selected), loads, err)
	}
}

func TestOutboxHydrationHandlesMissingErrorAndCancellation(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("storage unavailable")
	if _, err := LoadOutboxPayloads(ctx, []*ClaimedEvent{{}}, func(*ClaimedEvent) (bool, error) { return false, sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if selected, err := LoadOutboxPayloads(ctx, []*ClaimedEvent{{}}, func(*ClaimedEvent) (bool, error) { return false, nil }); err != nil || len(selected) != 0 {
		t.Fatal(selected, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := LoadOutboxPayloads(canceled, []*ClaimedEvent{{}}, func(*ClaimedEvent) (bool, error) { t.Fatal("read after cancellation"); return false, nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
