package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestListDeliveries_PagesInOrder(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.CreateSubscription(ctx, &Subscription{WebhookID: "wh-1", OwnerClientID: "owner", URL: "http://x", Secret: "s", IsActive: true}); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	// Two rows share a timestamp, so the delivery_id tiebreak is exercised; .1s vs .12s checks
	// that stored timestamps order correctly.
	at := []time.Time{base, base.Add(100 * time.Millisecond), base.Add(120 * time.Millisecond), base.Add(time.Second), base.Add(time.Second)}
	for i, deliveredAt := range at {
		d := &Delivery{DeliveryID: fmt.Sprintf("d-%d", i), WebhookID: "wh-1", EventID: "e", HazardID: "h", Status: "SUCCESS", DeliveredAt: deliveredAt}
		if err := s.InsertDelivery(ctx, d); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	token := ""
	for page := 0; ; page++ {
		deliveries, next, err := s.ListDeliveries(ctx, "wh-1", token, 2)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, d := range deliveries {
			got = append(got, d.DeliveryID)
		}
		if next == "" {
			break
		}
		token = next
	}

	want := []string{"d-4", "d-3", "d-2", "d-1", "d-0"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("deliveries = %v, want %v", got, want)
	}
}

func TestDecodeDeliveryCursor_RejectsGarbage(t *testing.T) {
	for _, token := range []string{"!!", "bm9waXBl", "eHw="} {
		if _, err := decodeDeliveryCursor(token); err == nil {
			t.Errorf("decodeDeliveryCursor(%q) succeeded", token)
		}
	}
}
