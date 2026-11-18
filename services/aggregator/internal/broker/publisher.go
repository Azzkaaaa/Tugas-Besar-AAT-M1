package broker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	outboxBatchSize = 20
	// outboxLease covers publishing a whole batch, each with a broker confirm; a publisher that dies
	// mid-batch leaves its records to the others after this long.
	outboxLease       = 30 * time.Second
	outboxBaseBackoff = time.Second
	outboxMaxBackoff  = time.Minute
)

type OutboxPublisher struct {
	cfg     *config.Config
	outbox  storage.Outbox
	conn    *amqp.Connection
	ch      *amqp.Channel
	returns <-chan amqp.Return
	// yielded is set when the loop handed its lease over (leader.MaybeYield), so Run returns.
	yielded bool
}

func NewOutboxPublisher(cfg *config.Config, outbox storage.Outbox) *OutboxPublisher {
	return &OutboxPublisher{
		cfg:    cfg,
		outbox: outbox,
	}
}

// Start runs the publisher in the background until ctx is done.
func (p *OutboxPublisher) Start(ctx context.Context) {
	go p.Run(ctx)
}

// Run publishes the outbox until ctx is done, reconnecting to RabbitMQ as needed, and returns
// after closing its connection. Under a leader it runs inside the leader's work, so it has stopped
// before leadership is handed over.
func (p *OutboxPublisher) Run(ctx context.Context) {
	defer p.close()
	for ctx.Err() == nil {
		p.close() // the previous connection, if it dropped
		if err := p.connect(); err != nil {
			slog.WarnContext(ctx, "RabbitMQ connection failed; retrying in 2s", "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
			continue
		}
		p.processOutboxLoop(ctx)
		if p.yielded {
			p.yielded = false
			return
		}
	}
}

func (p *OutboxPublisher) connect() error {
	conn, err := amqp.DialConfig(p.cfg.AMQPURL, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial:      amqp.DefaultDial(3 * time.Second),
	})
	if err != nil {
		return fmt.Errorf("failed connecting to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed opening RabbitMQ channel: %w", err)
	}

	// 1. Declare fanout exchange. Consumers declare and bind their own queues.
	err = ch.ExchangeDeclare(
		p.cfg.RabbitMQExchange, // name
		"fanout",               // type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // no-wait
		nil,                    // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed declaring RabbitMQ exchange %q: %w", p.cfg.RabbitMQExchange, err)
	}

	// 2. Enable publisher confirms
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed enabling publisher confirms: %w", err)
	}

	p.conn = conn
	p.ch = ch
	p.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	slog.Info("connected to RabbitMQ", "exchange", p.cfg.RabbitMQExchange)
	return nil
}

func (p *OutboxPublisher) close() {
	if p.conn != nil {
		_ = p.conn.CloseDeadline(time.Now().Add(time.Second))
	}
	p.ch = nil
	p.conn = nil
	p.returns = nil
}

func (p *OutboxPublisher) processOutboxLoop(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	closeChan := p.conn.NotifyClose(make(chan *amqp.Error, 1))

	maxRetries := p.cfg.OutboxMaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-closeChan:
			if ok && err != nil {
				slog.WarnContext(ctx, "RabbitMQ connection closed", "error", err)
			}
			return
		case <-ticker.C:
			records, err := p.outbox.ClaimDue(ctx, outboxBatchSize, outboxLease)
			if err != nil {
				slog.ErrorContext(ctx, "claiming outbox events failed", "error", err)
				continue
			}

			for i, rec := range records {
				pubErr := p.publishOne(ctx, rec)
				if pubErr != nil {
					if ctx.Err() != nil {
						// Shutting down: not the record's fault, hand the rest back at once.
						p.release(records[i:])
						return
					}
					slog.WarnContext(telemetry.ContextFromCarrier(ctx, rec.TraceContext), "publishing outbox event failed",
						"event_id", rec.EventID, "hazard_id", rec.HazardID, "correlation_id", rec.EventID, "retry", rec.RetryCount, "error", pubErr)

					if rec.RetryCount+1 >= maxRetries && rec.Source != domain.SourceCoastal {
						slog.ErrorContext(telemetry.ContextFromCarrier(ctx, rec.TraceContext), "outbox event dead-lettered after max retries", "event_id", rec.EventID, "hazard_id", rec.HazardID, "correlation_id", rec.EventID, "max_retries", maxRetries)
						_ = p.outbox.DeadLetter(ctx, rec, pubErr.Error())
					} else {
						_ = p.outbox.RetryLater(ctx, rec.EventID, pubErr.Error(), outboxBackoff(rec.RetryCount+1))
					}
					if p.ch.IsClosed() {
						// Reconnect; the records not yet attempted are due again at once.
						p.release(records[i+1:])
						return
					}
					// The record waits out its backoff; the ones behind it go ahead.
					continue
				}

				if err := p.outbox.Ack(ctx, rec.EventID); err != nil {
					slog.ErrorContext(telemetry.ContextFromCarrier(ctx, rec.TraceContext), "acknowledging published event failed", "event_id", rec.EventID, "correlation_id", rec.EventID, "error", err)
				} else {
					slog.InfoContext(telemetry.ContextFromCarrier(ctx, rec.TraceContext), "outbox event published",
						"event_id", rec.EventID, "hazard_id", rec.HazardID, "source", rec.Source, "correlation_id", rec.EventID)
				}
			}
			// Safe point: every claimed event was acked, retried or released. When the publisher
			// runs as a leader job it may hand its lease over here.
			if leader.MaybeYield(ctx) {
				p.yielded = true
				return
			}
		}
	}
}

// release makes claimed records due again without counting an attempt.
func (p *OutboxPublisher) release(records []*storage.ClaimedEvent) {
	if len(records) == 0 {
		return
	}
	ids := make([]string, len(records))
	for i, rec := range records {
		ids[i] = rec.EventID
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.outbox.Release(ctx, ids); err != nil {
		slog.Warn("releasing claimed events failed; they are due again when the lease ends", "events", len(ids), "error", err)
	}
}

// outboxBackoff is the wait before attempt+1: 1s, 2s, 4s, ... capped at outboxMaxBackoff.
func outboxBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 16 {
		return outboxMaxBackoff
	}
	return min(outboxBaseBackoff<<(attempt-1), outboxMaxBackoff)
}

func (p *OutboxPublisher) publishOne(ctx context.Context, rec *storage.ClaimedEvent) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	pubStart := time.Now()

	var ctxPub context.Context
	var span trace.Span
	if telemetry.Tracer != nil {
		// Continue the ingest poll's trace (stored with the row) rather than the poll loop's
		// empty context, so consumers join the trace that found the hazard.
		parent := telemetry.ContextFromCarrier(ctx, rec.TraceContext)
		ctxPub, span = telemetry.Tracer.Start(parent, "rabbitmq.publish", trace.WithSpanKind(trace.SpanKindProducer))
		defer span.End()
		span.SetAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.operation.type", "publish"),
			attribute.String("messaging.destination.name", p.cfg.RabbitMQExchange),
			attribute.String("messaging.message.id", rec.EventID),
			attribute.String("messaging.destination", p.cfg.RabbitMQExchange),
			attribute.String("messaging.message_id", rec.EventID),
			attribute.String("hazard.id", rec.HazardID),
			attribute.String("hazard.source", rec.Source),
		)
	} else {
		ctxPub = ctx
	}

	headers := telemetry.InjectAMQPHeaders(ctxPub, make(amqp.Table))

	message := amqp.Publishing{
		Headers:      headers,
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/protobuf",
		Body:         rec.Payload,
		Timestamp:    time.Now().UTC(),
		MessageId:    rec.EventID,
	}
	// A fanout confirmation can succeed while the notification queue is absent
	// (another subscriber is enough). A mandatory direct copy proves routing to
	// the required queue itself. Its consumer deduplicates the deterministic
	// event ID if it also receives the fanout copy or a publisher retry.
	var err error
	if p.cfg.RequiredNotificationQueue != "" {
		err = p.publishConfirmed(ctxPub, "", p.cfg.RequiredNotificationQueue, message)
	}
	if err == nil {
		err = p.publishConfirmed(ctxPub, p.cfg.RabbitMQExchange, "", message)
	}
	if err != nil {
		// A timed-out confirmation has an unknown outcome. Closing prevents its
		// delayed return/confirmation from being attributed to a later record.
		_ = p.conn.CloseDeadline(time.Now().Add(time.Second))
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		if telemetry.OutboxErrorsCounter != nil {
			telemetry.OutboxErrorsCounter.Add(ctxPub, 1, metric.WithAttributes(attribute.String("source", rec.Source)))
		}
		return err
	}

	durationSec := time.Since(pubStart).Seconds()
	if telemetry.OutboxDurationHistogram != nil {
		telemetry.OutboxDurationHistogram.Record(ctxPub, durationSec, metric.WithAttributes(attribute.String("source", rec.Source)))
	}
	if telemetry.OutboxPublishedCounter != nil {
		telemetry.OutboxPublishedCounter.Add(ctxPub, 1, metric.WithAttributes(attribute.String("source", rec.Source)))
	}

	return nil
}

func (p *OutboxPublisher) publishConfirmed(ctx context.Context, exchange, key string, message amqp.Publishing) error {
	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, true, false, message)
	if err != nil {
		return err
	}
	if confirmation == nil {
		return fmt.Errorf("publisher confirmation mode is unavailable")
	}
	accepted, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("publisher confirmation: %w", err)
	}
	if !accepted {
		return fmt.Errorf("broker rejected publication")
	}
	// RabbitMQ sends basic.return before basic.ack for an unroutable mandatory
	// publication. The buffered listener is filled before confirmation resolves.
	select {
	case returned, open := <-p.returns:
		if !open {
			return fmt.Errorf("publisher return channel closed")
		}
		return fmt.Errorf("publication was not routed (code=%d)", returned.ReplyCode)
	default:
		return nil
	}
}
