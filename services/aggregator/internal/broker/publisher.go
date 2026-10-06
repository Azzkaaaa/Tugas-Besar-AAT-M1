package broker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
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

	// 1. Declare fanout exchange
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

	// 2. Pre-declare consumer queues + dead letter queues and bind to fanout exchange
	for _, c := range p.cfg.ConsumerQueues {
		var queueArgs amqp.Table

		// If a DeadLetterQueue is configured for this consumer, declare it
		if c.DeadLetterQueue != "" {
			_, err = ch.QueueDeclare(
				c.DeadLetterQueue, // name
				true,              // durable
				false,             // auto-delete
				false,             // exclusive
				false,             // no-wait
				nil,               // arguments
			)
			if err != nil {
				ch.Close()
				conn.Close()
				return fmt.Errorf("failed declaring DLQ %q for consumer %q: %w", c.DeadLetterQueue, c.Name, err)
			}

			// Point the consumer queue's dead letter routing to the default exchange with routing key = DLQ name
			queueArgs = amqp.Table{
				"x-dead-letter-exchange":    "",
				"x-dead-letter-routing-key": c.DeadLetterQueue,
			}
		}

		if c.Queue != "" {
			_, err = ch.QueueDeclare(
				c.Queue,   // name
				true,      // durable
				false,     // auto-delete
				false,     // exclusive
				false,     // no-wait
				queueArgs, // arguments
			)
			if err != nil {
				ch.Close()
				conn.Close()
				return fmt.Errorf("failed declaring consumer queue %q: %w", c.Queue, err)
			}

			// Bind queue to the fanout exchange
			err = ch.QueueBind(
				c.Queue,                // queue name
				"",                     // routing key (ignored for fanout)
				p.cfg.RabbitMQExchange, // exchange
				false,                  // no-wait
				nil,                    // arguments
			)
			if err != nil {
				ch.Close()
				conn.Close()
				return fmt.Errorf("failed binding queue %q to exchange %q: %w", c.Queue, p.cfg.RabbitMQExchange, err)
			}
		}
	}

	// 3. Enable publisher confirms
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed enabling publisher confirms: %w", err)
	}

	p.conn = conn
	p.ch = ch
	log.Printf("[OutboxPublisher] Connected to RabbitMQ at %s. Exchange %q configured with %d consumer queues.", p.cfg.AMQPURL, p.cfg.RabbitMQExchange, len(p.cfg.ConsumerQueues))
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
			records, err := p.db.FetchUnpublishedOutbox(ctx, 20)
			if err != nil || len(records) == 0 {
				continue
			}

			for _, rec := range records {
				pubErr := p.publishOne(ctx, rec)
				if pubErr != nil {
					log.Printf("[OutboxPublisher] Failed to publish outbox event %s (hazard=%s, retry=%d): %v",
						rec.EventID, rec.HazardID, rec.RetryCount, pubErr)

					if rec.RetryCount+1 >= maxRetries {
						log.Printf("[OutboxPublisher] Outbox event %s reached max retries (%d), marking as failed", rec.EventID, maxRetries)
						_ = p.db.FailOutboxRecord(ctx, rec, pubErr.Error())
					} else {
						_ = p.db.IncrementOutboxRetry(ctx, rec.EventID, pubErr.Error())
					}
					// Return to allow reconnection if connection is severed
					return
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

func (p *OutboxPublisher) publishOne(ctx context.Context, rec *store.OutboxRecord) error {
	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(
		ctx,
		p.cfg.RabbitMQExchange, // exchange
		"",                     // routing key
		false,                  // mandatory
		false,                  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/protobuf",
			Body:         rec.Payload,
			Timestamp:    time.Now().UTC(),
			MessageId:    rec.EventID,
		},
	)
	if err != nil {
		return err
	}

	if confirmation != nil {
		ok, err := confirmation.WaitContext(ctx)
		if err != nil || !ok {
			return fmt.Errorf("publisher confirmation nack or error: %w", err)
		}
	}

	return nil
}

