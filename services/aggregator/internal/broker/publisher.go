package broker

import (
	"context"
	"fmt"
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

	// Declare fanout exchange
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
		return fmt.Errorf("failed declaring RabbitMQ exchange: %w", err)
	}

	// Enable publisher confirms
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed enabling publisher confirms: %w", err)
	}

	p.conn = conn
	p.ch = ch
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

	for {
		select {
		case <-ctx.Done():
			return
		case <-closeChan:
			return
		case <-ticker.C:
			records, err := p.db.FetchUnpublishedOutbox(ctx, 20)
			if err != nil || len(records) == 0 {
				continue
			}

			for _, rec := range records {
				err := p.publishOne(ctx, rec)
				if err != nil {
					// Channel or connection might be closed
					return
				}
				_ = p.db.MarkOutboxPublished(ctx, rec.EventID)
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
