package dedup

import (
	"context"
	"testing"
	"time"
)

func newTestMemory() (*Memory, *time.Time) {
	now := time.Unix(1_700_000_000, 0)
	m := NewMemory(time.Hour)
	m.now = func() time.Time { return now }
	return m, &now
}

func mustClaim(t *testing.T, m *Memory, want Result) Claim {
	t.Helper()
	claim, got, err := m.Claim(context.Background(), "evt-1", "wh-1", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if got != want {
		t.Fatalf("claim result = %v, want %v", got, want)
	}
	return claim
}

func TestMemory_SecondClaimIsHeldElsewhere(t *testing.T) {
	m, _ := newTestMemory()
	mustClaim(t, m, Claimed)
	mustClaim(t, m, HeldElsewhere)
}

func TestMemory_MarkDeliveredSkipsLaterClaims(t *testing.T) {
	m, now := newTestMemory()
	claim := mustClaim(t, m, Claimed)
	if err := m.MarkDelivered(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	mustClaim(t, m, AlreadyDelivered)

	*now = now.Add(2 * time.Hour)
	mustClaim(t, m, Claimed)
}

func TestMemory_ReleaseAllowsRetry(t *testing.T) {
	m, _ := newTestMemory()
	claim := mustClaim(t, m, Claimed)
	if err := m.Release(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	mustClaim(t, m, Claimed)
}

func TestMemory_ExpiredLeaseCanBeTakenOver(t *testing.T) {
	m, now := newTestMemory()
	mustClaim(t, m, Claimed)
	*now = now.Add(time.Minute)
	mustClaim(t, m, Claimed)
}

func TestMemory_StaleTokenCannotMarkOrRelease(t *testing.T) {
	m, now := newTestMemory()
	stale := mustClaim(t, m, Claimed)
	*now = now.Add(time.Minute)
	current := mustClaim(t, m, Claimed)

	_ = m.MarkDelivered(context.Background(), stale)
	_ = m.Release(context.Background(), stale)
	mustClaim(t, m, HeldElsewhere)

	_ = m.MarkDelivered(context.Background(), current)
	_ = m.Release(context.Background(), Claim{EventID: "evt-1", WebhookID: "wh-1"})
	mustClaim(t, m, AlreadyDelivered)
}
