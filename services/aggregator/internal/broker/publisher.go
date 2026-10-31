package broker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/store"
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
	cfg  *config.Config
	db   *store.DB
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewOutboxPublisher(cfg *config.Config, db *store.DB) *OutboxPublisher {
	return &OutboxPublisher{
		cfg: cfg,
		db:  db,
	}
}

func (p *OutboxPublisher) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				p.close()
				return
			default:
				if err := p.connect(); err != nil {
					log.Printf("[OutboxPublisher] Connection error: %v, retrying in 2s...", err)
					time.Sleep(2 * time.Second)
					continue
				}

				p.processOutboxLoop(ctx)
			}
		}
	}()
}

func (p *OutboxPublisher) connect() error {
	conn, err := amqp.Dial(p.cfg.AMQPURL)
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
	log.Printf("[OutboxPublisher] Connected to RabbitMQ at %s. Exchange %q declared.", p.cfg.AMQPURL, p.cfg.RabbitMQExchange)
	return nil
}

func (p *OutboxPublisher) close() {
	if p.ch != nil {
		_ = p.ch.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
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
				log.Printf("[OutboxPublisher] RabbitMQ connection closed: %v", err)
			}
			return
		case <-ticker.C:
			records, err := p.db.ClaimDueOutbox(ctx, outboxBatchSize, outboxLease)
			if err != nil {
				log.Printf("[OutboxPublisher] Failed claiming outbox events: %v", err)
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
					log.Printf("[OutboxPublisher] Failed to publish outbox event %s (hazard=%s, retry=%d): %v",
						rec.EventID, rec.HazardID, rec.RetryCount, pubErr)

					if rec.RetryCount+1 >= maxRetries {
						log.Printf("[OutboxPublisher] Outbox event %s reached max retries (%d), marking as failed", rec.EventID, maxRetries)
						_ = p.db.FailOutboxRecord(ctx, rec, pubErr.Error())
					} else {
						_ = p.db.RetryOutboxLater(ctx, rec.EventID, pubErr.Error(), outboxBackoff(rec.RetryCount+1))
					}
					if p.ch.IsClosed() {
						// Reconnect; the records not yet attempted are due again at once.
						p.release(records[i+1:])
						return
					}
					// The record waits out its backoff; the ones behind it go ahead.
					continue
				}

				if err := p.db.MarkOutboxPublished(ctx, rec.EventID); err != nil {
					log.Printf("[OutboxPublisher] Error marking event %s published: %v", rec.EventID, err)
				} else {
					log.Printf("[OutboxPublisher] Successfully published event %s (hazard=%s, source=%s)",
						rec.EventID, rec.HazardID, rec.Source)
				}
			}
		}
	}
}

// release makes claimed records due again without counting an attempt.
func (p *OutboxPublisher) release(records []*store.OutboxRecord) {
	if len(records) == 0 {
		return
	}
	ids := make([]string, len(records))
	for i, rec := range records {
		ids[i] = rec.EventID
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.db.ReleaseOutbox(ctx, ids); err != nil {
		log.Printf("[OutboxPublisher] Failed releasing %d claimed events (they are due again when the lease ends): %v", len(ids), err)
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

func (p *OutboxPublisher) publishOne(ctx context.Context, rec *store.OutboxRecord) error {
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

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(
		ctxPub,
		p.cfg.RabbitMQExchange, // exchange
		"",                     // routing key
		false,                  // mandatory
		false,                  // immediate
		amqp.Publishing{
			Headers:      headers,
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/protobuf",
			Body:         rec.Payload,
			Timestamp:    time.Now().UTC(),
			MessageId:    rec.EventID,
		},
	)
	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		if telemetry.OutboxErrorsCounter != nil {
			telemetry.OutboxErrorsCounter.Add(ctxPub, 1, metric.WithAttributes(attribute.String("source", rec.Source)))
		}
		return err
	}

	if confirmation != nil {
		ok, err := confirmation.WaitContext(ctxPub)
		if err != nil || !ok {
			nackErr := fmt.Errorf("publisher confirmation nack or error: %w", err)
			if span != nil {
				span.RecordError(nackErr)
				span.SetStatus(codes.Error, nackErr.Error())
			}
			if telemetry.OutboxErrorsCounter != nil {
				telemetry.OutboxErrorsCounter.Add(ctxPub, 1, metric.WithAttributes(attribute.String("source", rec.Source)))
			}
			return nackErr
		}
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
