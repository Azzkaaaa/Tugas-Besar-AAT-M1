//go:build integration

package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type routedOutbox struct {
	storage.Outbox
	record                 *storage.ClaimedEvent
	taken                  bool
	acked, retried, parked int
	cancel                 context.CancelFunc
}

func (o *routedOutbox) ClaimDue(context.Context, int, time.Duration) ([]*storage.ClaimedEvent, error) {
	if o.taken {
		return nil, nil
	}
	o.taken = true
	return []*storage.ClaimedEvent{o.record}, nil
}
func (o *routedOutbox) Ack(context.Context, string) error {
	o.acked++
	o.cancel()
	return nil
}
func (o *routedOutbox) RetryLater(context.Context, string, string, time.Duration) error {
	o.retried++
	o.cancel()
	return nil
}
func (o *routedOutbox) DeadLetter(context.Context, *storage.ClaimedEvent, string) error {
	o.parked++
	o.cancel()
	return nil
}
func (*routedOutbox) Release(context.Context, []string) error { return nil }

func TestPublicationRequiresTheNotificationQueueAndRecovers(t *testing.T) {
	url := os.Getenv("AGG_TEST_AMQP_URL")
	if url == "" {
		t.Fatal("AGG_TEST_AMQP_URL not set")
	}
	connection, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	admin, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	suffix := uuid.NewString()
	exchange, other, required := "test-exchange-"+suffix, "test-other-"+suffix, "test-notifications-"+suffix
	defer func() {
		_, _ = admin.QueueDelete(other, false, false, false)
		_, _ = admin.QueueDelete(required, false, false, false)
		_ = admin.ExchangeDelete(exchange, false, false)
	}()
	if err = admin.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.QueueDeclare(other, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err = admin.QueueBind(other, "", exchange, false, nil); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{AMQPURL: url, RabbitMQExchange: exchange, RequiredNotificationQueue: required, OutboxMaxRetries: 5}
	record := &storage.ClaimedEvent{OutboxEvent: storage.OutboxEvent{EventID: "critical-" + suffix, Source: domain.SourceCoastal, Payload: []byte("critical warning")}, RetryCount: 100}
	run := func() *routedOutbox {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		outbox := &routedOutbox{record: record, cancel: cancel}
		publisher := NewOutboxPublisher(cfg, outbox)
		publisher.Run(ctx)
		return outbox
	}
	failed := run()
	if failed.acked != 0 || failed.retried != 1 || failed.parked != 0 {
		t.Fatalf("missing required route was discarded or acknowledged: %+v", failed)
	}
	q, err := admin.QueueInspect(other)
	if err != nil || q.Messages != 0 {
		t.Fatal("unrelated fanout route masked failure", q, err)
	}
	// The notification queue is deliberately NOT bound to the fanout exchange:
	// success must establish the explicit route to this consumer's queue.
	if _, err = admin.QueueDeclare(required, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	recovered := run()
	if recovered.acked != 1 || recovered.retried != 0 || recovered.parked != 0 {
		t.Fatalf("recovery failed: %+v", recovered)
	}
	for _, queue := range []string{required, other} {
		message, ok, err := admin.Get(queue, true)
		if err != nil || !ok || message.MessageId != record.EventID || string(message.Body) != "critical warning" {
			t.Fatal("required/fanout delivery missing", queue, ok, message.MessageId, err)
		}
	}
	// Erasing/recreating the queue makes the same event retryable again.
	if _, err = admin.QueueDelete(required, false, false, false); err != nil {
		t.Fatal(err)
	}
	if erased := run(); erased.acked != 0 || erased.retried != 1 {
		t.Fatalf("erased queue accepted publication: %+v", erased)
	}
}

func TestUnroutedFanoutAndCancelledPublicationAreRejected(t *testing.T) {
	url := os.Getenv("AGG_TEST_AMQP_URL")
	if url == "" {
		t.Fatal("AGG_TEST_AMQP_URL not set")
	}
	cfg := &config.Config{AMQPURL: url, RabbitMQExchange: "test-unrouted-" + uuid.NewString()}
	p := NewOutboxPublisher(cfg, nil)
	if err := p.connect(); err != nil {
		t.Fatal(err)
	}
	defer p.close()
	cleanup, err := p.conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup.Close()
	defer cleanup.ExchangeDelete(cfg.RabbitMQExchange, false, false)
	record := &storage.ClaimedEvent{OutboxEvent: storage.OutboxEvent{EventID: "unrouted", Payload: []byte("body")}}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.publishOne(cancelled, record); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled work accepted", err)
	}
	if err := p.publishOne(context.Background(), record); err == nil || !strings.Contains(err.Error(), "not routed") {
		t.Fatal("unrouted confirmation was treated as delivery", err)
	}
}

type heldWriter struct {
	io.Writer
	hold    *atomic.Bool
	release <-chan struct{}
}

func (w heldWriter) Write(data []byte) (int, error) {
	if w.hold.Load() {
		<-w.release
	}
	return w.Writer.Write(data)
}

func TestConfirmationTimeoutClosesTheConnectionWithinBudget(t *testing.T) {
	url := os.Getenv("AGG_TEST_AMQP_URL")
	if url == "" {
		t.Fatal("AGG_TEST_AMQP_URL not set")
	}
	adminConnection, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer adminConnection.Close()
	admin, err := adminConnection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	queue, err := admin.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := amqp.ParseURI(url)
	if err != nil || uri.Scheme != "amqp" {
		t.Fatal("fixture requires a plain AMQP URL", err)
	}
	upstreamAddress := net.JoinHostPort(uri.Host, fmt.Sprint(uri.Port))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var hold atomic.Bool
	release, done := make(chan struct{}), make(chan struct{})
	proxyErrors := make(chan error, 1)
	go func() {
		defer close(done)
		client, err := listener.Accept()
		if err != nil {
			proxyErrors <- err
			return
		}
		defer client.Close()
		upstream, err := net.DialTimeout("tcp", upstreamAddress, time.Second)
		if err != nil {
			proxyErrors <- err
			return
		}
		defer upstream.Close()
		go func() { _, _ = io.Copy(upstream, client) }()
		_, _ = io.Copy(heldWriter{Writer: client, hold: &hold, release: release}, upstream)
	}()
	uri.Host = "127.0.0.1"
	uri.Port = listener.Addr().(*net.TCPAddr).Port
	cfg := &config.Config{AMQPURL: uri.String(), RabbitMQExchange: "test-timeout-" + uuid.NewString(), RequiredNotificationQueue: queue.Name}
	defer admin.ExchangeDelete(cfg.RabbitMQExchange, false, false)
	p := NewOutboxPublisher(cfg, nil)
	t.Cleanup(func() {
		close(release)
		_ = listener.Close()
		p.close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("proxy did not stop")
		}
	})
	if err := p.connect(); err != nil {
		t.Fatal(err)
	}
	// Hold broker replies after the AMQP handshake. The broker can accept the
	// publication while its confirmation (and close response) never reaches us.
	hold.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = p.publishOne(ctx, &storage.ClaimedEvent{OutboxEvent: storage.OutboxEvent{EventID: "unknown-outcome", Payload: []byte("warning")}})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second || !p.ch.IsClosed() {
		t.Fatalf("unbounded/accepted unknown outcome: %v after %s closed=%v", err, time.Since(start), p.ch.IsClosed())
	}
	message, received, err := admin.Get(queue.Name, true)
	if err != nil || !received || message.MessageId != "unknown-outcome" {
		t.Fatal("fixture did not persist the unconfirmed event", received, err)
	}
	select {
	case e := <-proxyErrors:
		t.Fatal(e)
	default:
	}
}
