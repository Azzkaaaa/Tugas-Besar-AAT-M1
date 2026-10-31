// Package dedup claims (event, webhook) deliveries so that replicas sharing the consumer queue
// dispatch each event to each webhook at most once at a time, and skip events already delivered.
//
// Claims are best-effort: the backing store may lose them (Redis runs without persistence), in
// which case a redelivered event can be sent again. Receivers dedupe by event_id.
package dedup

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Result is the outcome of a claim attempt.
type Result int

const (
	// Claimed: the caller owns the delivery until it marks, releases, or the lease runs out.
	Claimed Result = iota
	// AlreadyDelivered: an earlier dispatch succeeded; skip.
	AlreadyDelivered
	// HeldElsewhere: another consumer is dispatching right now.
	HeldElsewhere
)

func (r Result) String() string {
	switch r {
	case Claimed:
		return "claimed"
	case AlreadyDelivered:
		return "already_delivered"
	case HeldElsewhere:
		return "held_elsewhere"
	default:
		return "unknown"
	}
}

// Claim identifies one consumer's hold on a delivery. Only its holder can mark or release it.
type Claim struct {
	EventID   string
	WebhookID string
	Token     string
}

// Claimer is implemented by the Redis and the in-memory backends.
type Claimer interface {
	// Claim tries to take the delivery for lease. The returned Claim is only usable when the
	// result is Claimed.
	Claim(ctx context.Context, eventID, webhookID string, lease time.Duration) (Claim, Result, error)
	// MarkDelivered records a successful dispatch; later claims return AlreadyDelivered.
	// It does nothing if the claim was lost (lease expired and taken over).
	MarkDelivered(ctx context.Context, claim Claim) error
	// Release gives the delivery up after a failed dispatch, so a redelivery can retry it.
	Release(ctx context.Context, claim Claim) error
	Close() error
}

func newClaim(eventID, webhookID string) Claim {
	return Claim{EventID: eventID, WebhookID: webhookID, Token: uuid.NewString()}
}
