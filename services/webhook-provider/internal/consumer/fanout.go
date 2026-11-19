package consumer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/dedup"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/dispatcher"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/store"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/telemetry"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const fanoutWorkers = 4

type deliveryOutcome struct{ failed, interrupted bool }
type payloadValue struct {
	once sync.Once
	body []byte
	err  error
}
type eventPayloads struct {
	event         *eventv1.HazardEventPublished
	summary, full payloadValue
}

func (p *eventPayloads) get(view commonv1.View) ([]byte, error) {
	value := &p.summary
	if view == commonv1.View_VIEW_FULL {
		value = &p.full
	}
	value.once.Do(func() { value.body, value.err = dispatcher.BuildPayload(p.event, view) })
	return value.body, value.err
}

func (c *Consumer) fanout(ctx context.Context, event *eventv1.HazardEventPublished, span trace.Span) deliveryOutcome {
	logf := telemetry.Logf(ctx)
	head, err := c.db.ActiveSubscriptionHead(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return deliveryOutcome{interrupted: true}
		}
		logf("[Consumer] Failed opening subscription walk for event %s: %v", event.EventId, err)
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return deliveryOutcome{failed: true}
	}
	jobs := make(chan *store.Subscription, fanoutWorkers)
	var failed, interrupted atomic.Bool
	var workers sync.WaitGroup
	payloads := &eventPayloads{event: event}
	for range fanoutWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for sub := range jobs {
				if ctx.Err() != nil {
					interrupted.Store(true)
					continue
				}
				outcome := c.deliver(ctx, event, sub, payloads)
				if outcome.failed {
					failed.Store(true)
				}
				if outcome.interrupted {
					interrupted.Store(true)
				}
			}
		}()
	}
walk:
	for after := int64(0); after < head; {
		subs, next, err := c.db.ListActiveSubscriptions(ctx, after, head)
		if err == nil && (next <= after || next > head) {
			err = fmt.Errorf("subscription walk did not advance within its boundary")
		}
		if err != nil {
			if ctx.Err() != nil {
				interrupted.Store(true)
			} else {
				failed.Store(true)
				logf("[Consumer] Failed reading subscription batch for event %s: %v", event.EventId, err)
				if span != nil {
					span.RecordError(err)
				}
			}
			break
		}
		for _, sub := range subs {
			if !dispatcher.MatchesFilter(event, sub.FilterSource, sub.FilterHazard, sub.FilterSeverity, sub.FilterSince, sub.FilterUntil) {
				continue
			}
			select {
			case jobs <- sub:
			case <-ctx.Done():
				interrupted.Store(true)
				break walk
			}
		}
		after = next
	}
	close(jobs)
	workers.Wait() // ACK/admission cannot outlive dispatch/cleanup goroutines.
	return deliveryOutcome{failed: failed.Load(), interrupted: interrupted.Load() || ctx.Err() != nil}
}

func (c *Consumer) deliver(ctx context.Context, event *eventv1.HazardEventPublished, sub *store.Subscription, payloads *eventPayloads) deliveryOutcome {
	logf := telemetry.Logf(ctx)
	claim, result, err := c.claim(ctx, event.EventId, sub.WebhookID)
	if err != nil && ctx.Err() != nil {
		return deliveryOutcome{interrupted: true}
	}
	if err != nil {
		logf("[Consumer] Dedup claim failed for event %s / webhook %s: %v", event.EventId, sub.WebhookID, err)
		return deliveryOutcome{failed: true}
	}
	switch result {
	case dedup.AlreadyDelivered:
		return deliveryOutcome{}
	case dedup.HeldElsewhere:
		logf("[Consumer] Event %s / webhook %s still claimed elsewhere", event.EventId, sub.WebhookID)
		return deliveryOutcome{failed: true}
	}
	payload, err := payloads.get(commonv1.View(sub.View))
	if err != nil {
		logf("[Consumer] Failed building payload for event %s / webhook %s: %v", event.EventId, sub.WebhookID, err)
		c.release(ctx, claim, logf)
		return deliveryOutcome{failed: true}
	}
	resultHTTP := c.disp.Dispatch(ctx, sub.URL, sub.Secret, payload)
	status := "FAILED"
	if resultHTTP.Err == nil && resultHTTP.StatusCode >= 200 && resultHTTP.StatusCode < 300 {
		status = "SUCCESS"
	}
	if status != "SUCCESS" && ctx.Err() != nil {
		c.release(ctx, claim, logf)
		return deliveryOutcome{interrupted: true}
	}
	errMessage := ""
	if resultHTTP.Err != nil {
		errMessage = resultHTTP.Err.Error()
	}
	delivery := &store.Delivery{DeliveryID: uuid.NewString(), WebhookID: sub.WebhookID, EventID: event.EventId,
		HazardID: event.GetPayload().GetHazardId(), StatusCode: int32(resultHTTP.StatusCode), Status: status,
		Attempt: int32(c.cfg.MaxDeliveryRetries), DurationMS: resultHTTP.DurationMS,
		ErrorMessage: errMessage, DeliveredAt: time.Now().UTC()}
	if status == "SUCCESS" {
		markCtx, cancel := cleanupContext(ctx)
		if err := c.claims.MarkDelivered(markCtx, claim); err != nil {
			logf("[Consumer] Failed marking delivered for event %s / webhook %s: %v", event.EventId, sub.WebhookID, err)
		}
		cancel()
	} else {
		c.release(ctx, claim, logf)
	}
	logCtx, cancel := cleanupContext(ctx)
	err = c.db.InsertDelivery(logCtx, delivery)
	cancel()
	if err != nil {
		logf("[Consumer] Failed inserting delivery log for event %s / webhook %s: %v", event.EventId, sub.WebhookID, err)
		return deliveryOutcome{failed: true}
	}
	// Completed receiver failures keep their existing logged/ACKed semantics.
	return deliveryOutcome{}
}
