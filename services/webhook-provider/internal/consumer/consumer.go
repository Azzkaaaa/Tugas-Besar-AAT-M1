package consumer

import (
	"context"
	"log"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/config"
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

// Consumer listens on the RabbitMQ queue and dispatches events to matching webhooks.
type Consumer struct {
	cfg  *config.Config
	db   *store.DB
	disp *dispatcher.Dispatcher
}

// New creates a Consumer.
func New(cfg *config.Config, db *store.DB, disp *dispatcher.Dispatcher) *Consumer {
	return &Consumer{cfg: cfg, db: db, disp: disp}
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
		logf("[Consumer] Failed to list subscriptions for event %s: %v — requeuing", ev.EventId, err)
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		if telemetry.ConsumerErrorsCounter != nil {
			telemetry.ConsumerErrorsCounter.Add(ctxMsg, 1)
		}
		_ = msg.Nack(false, true)
		return
	}

	allOK := true
	for _, sub := range subs {
		if !dispatcher.MatchesFilter(&ev, sub.FilterSource, sub.FilterHazard, sub.FilterSeverity, sub.FilterSince, sub.FilterUntil) {
			continue
		}

		// Idempotency: skip if already successfully delivered.
		delivered, err := c.db.IsDelivered(ctxMsg, ev.EventId, sub.WebhookID)
		if err != nil {
			logf("[Consumer] Dedup check failed for event %s / webhook %s: %v", ev.EventId, sub.WebhookID, err)
			allOK = false
			continue
		}
		if delivered {
			logf("[Consumer] Event %s already delivered to webhook %s — skipping", ev.EventId, sub.WebhookID)
			continue
		}

		view := commonv1.View(sub.View)
		payload, err := dispatcher.BuildPayload(&ev, view)
		if err != nil {
			logf("[Consumer] Failed building payload for event %s / webhook %s: %v", ev.EventId, sub.WebhookID, err)
			allOK = false
			continue
		}

		result := c.disp.Dispatch(ctxMsg, sub.URL, sub.Secret, payload)

		deliveryID := uuid.NewString()
		status := "FAILED"
		if result.Err == nil && result.StatusCode >= 200 && result.StatusCode < 300 {
			status = "SUCCESS"
		}
		errMsg := ""
		if result.Err != nil {
			errMsg = result.Err.Error()
		}

		d := &store.Delivery{
			DeliveryID:   deliveryID,
			WebhookID:    sub.WebhookID,
			EventID:      ev.EventId,
			HazardID:     ev.GetPayload().GetHazardId(),
			StatusCode:   int32(result.StatusCode),
			Status:       status,
			Attempt:      int32(c.cfg.MaxDeliveryRetries),
			DurationMS:   result.DurationMS,
			ErrorMessage: errMsg,
			DeliveredAt:  time.Now().UTC(),
		}

		if err := c.db.InsertDelivery(ctxMsg, d); err != nil {
			logf("[Consumer] Failed inserting delivery log for event %s / webhook %s: %v", ev.EventId, sub.WebhookID, err)
			allOK = false
			continue
		}

		if status == "SUCCESS" {
			if err := c.db.MarkDelivered(ctxMsg, ev.EventId, sub.WebhookID); err != nil {
				logf("[Consumer] Failed marking delivered for event %s / webhook %s: %v", ev.EventId, sub.WebhookID, err)
			}
		} else {
			logf("[Consumer] Delivery failed for event %s to webhook %s (url=%s): status=%d err=%v",
				ev.EventId, sub.WebhookID, sub.URL, result.StatusCode, result.Err)
			allOK = false
		}
	}

	// Ack after all side-effects have been stored (at-least-once semantics).
	// If any side-effect failed (DB write error), we nack with requeue so we retry.
	// Dispatch failures alone do not block the ack — they are recorded in the delivery log.
	if !allOK {
		logf("[Consumer] Partial failure processing event %s — nacking with requeue", ev.EventId)
		_ = msg.Nack(false, true)
	} else {
		_ = msg.Ack(false)
	}
}
