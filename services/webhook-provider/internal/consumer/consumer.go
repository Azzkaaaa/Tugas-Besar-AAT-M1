package consumer

import (
	"context"
	"log"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/dedup"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/dispatcher"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/store"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/telemetry"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// claimPollInterval is how often a consumer re-checks a delivery another replica is dispatching.
const claimPollInterval = time.Second

// Consumer listens on the RabbitMQ queue and dispatches events to matching webhooks.
type Consumer struct {
	cfg    *config.Config
	db     store.Store
	claims dedup.Claimer
	disp   *dispatcher.Dispatcher
	lease  time.Duration
}

// New creates a Consumer.
func New(cfg *config.Config, db store.Store, claims dedup.Claimer, disp *dispatcher.Dispatcher) *Consumer {
	return &Consumer{cfg: cfg, db: db, claims: claims, disp: disp, lease: claimLease(cfg)}
}

// claimLease covers the dispatcher's worst case: every attempt timing out plus its backoff
// (1s, 2s, 4s, ...), with a margin for logging the delivery.
func claimLease(cfg *config.Config) time.Duration {
	retries := max(cfg.MaxDeliveryRetries, 0)
	backoff := time.Duration(1<<uint(min(retries, 20))) * time.Second
	return time.Duration(retries+1)*cfg.HTTPTimeoutMS + backoff + 30*time.Second
}

// claim takes the (event, webhook) delivery. While another replica holds it (a redelivery after
// a channel loss), it waits until that replica marks, releases, or its lease runs out.
func (c *Consumer) claim(ctx context.Context, eventID, webhookID string) (dedup.Claim, dedup.Result, error) {
	deadline := time.Now().Add(c.lease)
	for {
		claim, result, err := c.claims.Claim(ctx, eventID, webhookID, c.lease)
		if err != nil || result != dedup.HeldElsewhere || time.Now().After(deadline) {
			return claim, result, err
		}
		select {
		case <-ctx.Done():
			return dedup.Claim{}, result, ctx.Err()
		case <-time.After(claimPollInterval):
		}
	}
}

// Start runs the consumer loop until ctx is cancelled. It reconnects automatically
// on connection loss (mirrors the aggregator's OutboxPublisher pattern).
func (c *Consumer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, ch, err := c.connect()
		if err != nil {
			log.Printf("[Consumer] Connection error: %v — retrying in 2s", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		log.Printf("[Consumer] Connected to RabbitMQ, consuming from %s", c.cfg.ConsumerQueue)
		c.consumeLoop(ctx, conn, ch)

		ch.Close()
		conn.Close()
	}
}

// connect dials RabbitMQ, declares the exchange, DLQ, consumer queue, and starts consuming.
func (c *Consumer) connect() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(c.cfg.AMQPURL)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	// Declare fanout exchange (idempotent — aggregator also declares it).
	if err := ch.ExchangeDeclare(
		c.cfg.RabbitMQExchange, "fanout", true, false, false, false, nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, err
	}

	// Dead-letter queue (no binding to exchange — receives nacked messages via x-dead-letter args).
	if _, err := ch.QueueDeclare(c.cfg.ConsumerDLQ, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, err
	}

	// Consumer queue with DLQ routing.
	queueArgs := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": c.cfg.ConsumerDLQ,
	}
	if _, err := ch.QueueDeclare(c.cfg.ConsumerQueue, true, false, false, false, queueArgs); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, err
	}

	// Bind consumer queue to exchange.
	if err := ch.QueueBind(c.cfg.ConsumerQueue, "", c.cfg.RabbitMQExchange, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, err
	}

	// Prefetch 1 — process one message at a time for simplicity.
	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, err
	}

	return conn, ch, nil
}

// consumeLoop processes messages until ctx is done or the channel closes.
func (c *Consumer) consumeLoop(ctx context.Context, conn *amqp.Connection, ch *amqp.Channel) {
	deliveries, err := ch.Consume(c.cfg.ConsumerQueue, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("[Consumer] Failed to start consuming: %v", err)
		return
	}

	closeChan := conn.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case <-ctx.Done():
			return
		case amqpErr, ok := <-closeChan:
			if ok && amqpErr != nil {
				log.Printf("[Consumer] RabbitMQ connection closed: %v", amqpErr)
			}
			return
		case msg, ok := <-deliveries:
			if !ok {
				return
			}
			c.handleMessage(ctx, msg)
		}
	}
}

// handleMessage processes a single RabbitMQ delivery.
func (c *Consumer) handleMessage(ctx context.Context, msg amqp.Delivery) {
	ctxMsg := telemetry.ExtractAMQPHeaders(ctx, msg.Headers)

	var span trace.Span
	if telemetry.Tracer != nil {
		// Child of the aggregator's rabbitmq.publish span; malformed headers start a new trace.
		ctxMsg, span = telemetry.Tracer.Start(ctxMsg, "rabbitmq.consume", trace.WithSpanKind(trace.SpanKindConsumer))
		defer span.End()
		span.SetAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.destination.name", c.cfg.ConsumerQueue),
			attribute.String("messaging.message.id", msg.MessageId),
			attribute.String("messaging.source", c.cfg.ConsumerQueue),
			attribute.String("messaging.message_id", msg.MessageId),
		)
	}
	logf := telemetry.Logf(ctxMsg)

	var ev eventv1.HazardEventPublished
	if err := proto.Unmarshal(msg.Body, &ev); err != nil {
		logf("[Consumer] Failed to unmarshal message: %v — dead-lettering", err)
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "unmarshal failure")
		}
		if telemetry.ConsumerErrorsCounter != nil {
			telemetry.ConsumerErrorsCounter.Add(ctxMsg, 1)
		}
		_ = msg.Nack(false, false)
		return
	}

	if span != nil {
		span.SetAttributes(
			attribute.String("hazard.id", ev.GetPayload().GetHazardId()),
			attribute.String("hazard.source", ev.GetPayload().GetSource().String()),
			attribute.Int64("schema.version", int64(ev.SchemaVersion)),
		)
	}

	if ev.SchemaVersion != 1 {
		logf("[Consumer] Unknown schema_version=%d for event %s — dead-lettering", ev.SchemaVersion, ev.EventId)
		if telemetry.ConsumerErrorsCounter != nil {
			telemetry.ConsumerErrorsCounter.Add(ctxMsg, 1)
		}
		_ = msg.Nack(false, false)
		return
	}

	if telemetry.EventsConsumedCounter != nil {
		telemetry.EventsConsumedCounter.Add(ctxMsg, 1, metric.WithAttributes(
			attribute.String("source", ev.GetPayload().GetSource().String()),
			attribute.String("hazard_type", ev.GetPayload().GetHazardType().String()),
		))
	}

	subs, err := c.db.ListActiveSubscriptions(ctxMsg)
	if err != nil {
		requeue := !msg.Redelivered
		logf("[Consumer] Failed to list subscriptions for event %s: %v (requeue=%v)", ev.EventId, err, requeue)
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		if telemetry.ConsumerErrorsCounter != nil {
			telemetry.ConsumerErrorsCounter.Add(ctxMsg, 1)
		}
		_ = msg.Nack(false, requeue)
		return
	}

	allOK := true
	for _, sub := range subs {
		if !dispatcher.MatchesFilter(&ev, sub.FilterSource, sub.FilterHazard, sub.FilterSeverity, sub.FilterSince, sub.FilterUntil) {
			continue
		}

		// Idempotency: claim the delivery; skip it if it already succeeded.
		claim, result, err := c.claim(ctxMsg, ev.EventId, sub.WebhookID)
		if err != nil {
			logf("[Consumer] Dedup claim failed for event %s / webhook %s: %v", ev.EventId, sub.WebhookID, err)
			allOK = false
			continue
		}
		switch result {
		case dedup.AlreadyDelivered:
			logf("[Consumer] Event %s already delivered to webhook %s — skipping", ev.EventId, sub.WebhookID)
			continue
		case dedup.HeldElsewhere:
			logf("[Consumer] Event %s / webhook %s still claimed elsewhere after %s", ev.EventId, sub.WebhookID, c.lease)
			allOK = false
			continue
		}

		view := commonv1.View(sub.View)
		payload, err := dispatcher.BuildPayload(&ev, view)
		if err != nil {
			logf("[Consumer] Failed building payload for event %s / webhook %s: %v", ev.EventId, sub.WebhookID, err)
			c.release(ctxMsg, claim, logf)
			allOK = false
			continue
		}

		dispatchResult := c.disp.Dispatch(ctxMsg, sub.URL, sub.Secret, payload)

		deliveryID := uuid.NewString()
		status := "FAILED"
		if dispatchResult.Err == nil && dispatchResult.StatusCode >= 200 && dispatchResult.StatusCode < 300 {
			status = "SUCCESS"
		}
		errMsg := ""
		if dispatchResult.Err != nil {
			errMsg = dispatchResult.Err.Error()
		}

		d := &store.Delivery{
			DeliveryID:   deliveryID,
			WebhookID:    sub.WebhookID,
			EventID:      ev.EventId,
			HazardID:     ev.GetPayload().GetHazardId(),
			StatusCode:   int32(dispatchResult.StatusCode),
			Status:       status,
			Attempt:      int32(c.cfg.MaxDeliveryRetries),
			DurationMS:   dispatchResult.DurationMS,
			ErrorMessage: errMsg,
			DeliveredAt:  time.Now().UTC(),
		}

		// Mark before logging: a successful send must not be repeated even if the log write fails.
		if status == "SUCCESS" {
			if err := c.claims.MarkDelivered(ctxMsg, claim); err != nil {
				logf("[Consumer] Failed marking delivered for event %s / webhook %s: %v", ev.EventId, sub.WebhookID, err)
			}
		} else {
			c.release(ctxMsg, claim, logf)
		}

		if err := c.db.InsertDelivery(ctxMsg, d); err != nil {
			logf("[Consumer] Failed inserting delivery log for event %s / webhook %s: %v", ev.EventId, sub.WebhookID, err)
			allOK = false
			continue
		}

		if status != "SUCCESS" {
			logf("[Consumer] Delivery failed for event %s to webhook %s (url=%s): status=%d err=%v",
				ev.EventId, sub.WebhookID, sub.URL, dispatchResult.StatusCode, dispatchResult.Err)
			// Dispatch failures alone do not block the ack — they have already completed their own retries
			// and are recorded in the delivery log.
		}
	}

	// Ack after all side-effects have been stored (at-least-once semantics).
	// If any internal side-effect failed (DB write error), we nack with requeue if not redelivered (one redelivery),
	// so persistent failures reach the DLQ instead of looping forever.
	// Dispatch failures alone do not block the ack — they are recorded in the delivery log.
	if !allOK {
		requeue := !msg.Redelivered
		logf("[Consumer] Partial failure processing event %s (requeue=%v) — nacking", ev.EventId, requeue)
		if span != nil {
			span.SetStatus(codes.Error, "partial failure storing side-effects")
		}
		if telemetry.ConsumerErrorsCounter != nil {
			telemetry.ConsumerErrorsCounter.Add(ctxMsg, 1)
		}
		_ = msg.Nack(false, requeue)
	} else {
		_ = msg.Ack(false)
	}
}

// release gives a claim up after a failed dispatch. A failure only delays a retry until the lease expires.
func (c *Consumer) release(ctx context.Context, claim dedup.Claim, logf func(string, ...any)) {
	if err := c.claims.Release(ctx, claim); err != nil {
		logf("[Consumer] Failed releasing claim for event %s / webhook %s: %v", claim.EventID, claim.WebhookID, err)
	}
}
