package consumer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/dedup"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/dispatcher"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockAcknowledger struct {
	ackCount      int
	nackCount     int
	nackedRequeue bool
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error {
	m.ackCount++
	return nil
}

func (m *mockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	m.nackCount++
	m.nackedRequeue = requeue
	return nil
}

func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error {
	return m.Nack(tag, false, requeue)
}

func sampleEvent(t *testing.T, schemaVersion uint32) []byte {
	t.Helper()
	ev := &eventv1.HazardEventPublished{
		EventId:       "evt-123",
		SchemaVersion: schemaVersion,
		Source:        commonv1.Source_SOURCE_BMKG,
		OccurredAt:    timestamppb.Now(),
		Payload: &commonv1.HazardEvent{
			HazardId:    "haz-123",
			Source:      commonv1.Source_SOURCE_BMKG,
			SourceRefId: "ref-123",
			HazardType:  commonv1.HazardType_HAZARD_TYPE_SEISMIC,
			Severity:    commonv1.Severity_SEVERITY_SIAGA,
			AreaName:    "Jakarta",
			OccurredAt:  timestamppb.Now(),
			IngestedAt:  timestamppb.Now(),
		},
	}
	body, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	return body
}

func TestHandleMessage_DispatchFailureAcksMessage(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer db.Close()

	// Server that always returns 500 Internal Server Error
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	// Add subscription pointing to failing server
	sub := &store.Subscription{
		WebhookID:     "wh-fail-1",
		OwnerClientID: "owner-1",
		URL:           ts.URL,
		Secret:        "secret-1",
		View:          int32(commonv1.View_VIEW_SUMMARY),
		IsActive:      true,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	if err := db.CreateSubscription(ctx, sub); err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}

	cfg := &config.Config{
		MaxDeliveryRetries: 1,
		HTTPTimeoutMS:      1 * time.Second,
	}
	disp := dispatcher.New(cfg.HTTPTimeoutMS, cfg.MaxDeliveryRetries)
	c := New(cfg, db, dedup.NewMemory(time.Hour), disp)

	ack := &mockAcknowledger{}
	msg := amqp.Delivery{
		Acknowledger: ack,
		Body:         sampleEvent(t, 1),
		MessageId:    "msg-1",
		Redelivered:  false,
	}

	c.handleMessage(ctx, msg)

	// Dispatch failures must NOT block the ACK: the delivery failure has already been retried
	// and stored in the database. Nacking would cause an infinite retry loop.
	if ack.ackCount != 1 {
		t.Errorf("expected 1 Ack, got %d", ack.ackCount)
	}
	if ack.nackCount != 0 {
		t.Errorf("expected 0 Nack, got %d (requeue=%v)", ack.nackCount, ack.nackedRequeue)
	}

	// Verify delivery log exists with status FAILED
	deliveries, _, err := db.ListDeliveries(ctx, sub.WebhookID, "", 10)
	if err != nil {
		t.Fatalf("failed to list deliveries: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery log, got %d", len(deliveries))
	}
	if deliveries[0].Status != "FAILED" {
		t.Errorf("expected delivery status FAILED, got %s", deliveries[0].Status)
	}
}

func TestHandleMessage_InternalFailure_RequeuesFirstTime(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	// Close DB so ListActiveSubscriptions fails with internal error
	db.Close()

	cfg := &config.Config{MaxDeliveryRetries: 1, HTTPTimeoutMS: 1 * time.Second}
	disp := dispatcher.New(cfg.HTTPTimeoutMS, cfg.MaxDeliveryRetries)
	c := New(cfg, db, dedup.NewMemory(time.Hour), disp)

	ack := &mockAcknowledger{}
	msg := amqp.Delivery{
		Acknowledger: ack,
		Body:         sampleEvent(t, 1),
		MessageId:    "msg-1",
		Redelivered:  false, // first attempt
	}

	c.handleMessage(ctx, msg)

	if ack.nackCount != 1 {
		t.Fatalf("expected 1 Nack, got %d", ack.nackCount)
	}
	// First time internal failure gets one redelivery
	if !ack.nackedRequeue {
		t.Errorf("expected requeue=true on first attempt, got false")
	}
}

func TestHandleMessage_InternalFailure_DeadLettersOnRedelivery(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	// Close DB so ListActiveSubscriptions fails with internal error
	db.Close()

	cfg := &config.Config{MaxDeliveryRetries: 1, HTTPTimeoutMS: 1 * time.Second}
	disp := dispatcher.New(cfg.HTTPTimeoutMS, cfg.MaxDeliveryRetries)
	c := New(cfg, db, dedup.NewMemory(time.Hour), disp)

	ack := &mockAcknowledger{}
	msg := amqp.Delivery{
		Acknowledger: ack,
		Body:         sampleEvent(t, 1),
		MessageId:    "msg-1",
		Redelivered:  true, // redelivery attempt
	}

	c.handleMessage(ctx, msg)

	if ack.nackCount != 1 {
		t.Fatalf("expected 1 Nack, got %d", ack.nackCount)
	}
	// Redelivered message must NOT be requeued again: routed to DLQ!
	if ack.nackedRequeue {
		t.Errorf("expected requeue=false on redelivery (dead-lettered), got true")
	}
}

func TestHandleMessage_UnmarshalFailure_DeadLetters(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer db.Close()

	cfg := &config.Config{MaxDeliveryRetries: 1, HTTPTimeoutMS: 1 * time.Second}
	disp := dispatcher.New(cfg.HTTPTimeoutMS, cfg.MaxDeliveryRetries)
	c := New(cfg, db, dedup.NewMemory(time.Hour), disp)

	ack := &mockAcknowledger{}
	msg := amqp.Delivery{
		Acknowledger: ack,
		Body:         []byte("invalid proto payload"),
		MessageId:    "msg-bad",
		Redelivered:  false,
	}

	c.handleMessage(ctx, msg)

	if ack.nackCount != 1 {
		t.Fatalf("expected 1 Nack, got %d", ack.nackCount)
	}
	if ack.nackedRequeue {
		t.Errorf("unmarshal error must be dead-lettered immediately (requeue=false)")
	}
}

func TestHandleMessage_SchemaVersionMismatch_DeadLetters(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer db.Close()

	cfg := &config.Config{MaxDeliveryRetries: 1, HTTPTimeoutMS: 1 * time.Second}
	disp := dispatcher.New(cfg.HTTPTimeoutMS, cfg.MaxDeliveryRetries)
	c := New(cfg, db, dedup.NewMemory(time.Hour), disp)

	ack := &mockAcknowledger{}
	msg := amqp.Delivery{
		Acknowledger: ack,
		Body:         sampleEvent(t, 99), // unsupported schema version
		MessageId:    "msg-schema-99",
		Redelivered:  false,
	}

	c.handleMessage(ctx, msg)

	if ack.nackCount != 1 {
		t.Fatalf("expected 1 Nack, got %d", ack.nackCount)
	}
	if ack.nackedRequeue {
		t.Errorf("unsupported schema version must be dead-lettered immediately (requeue=false)")
	}
}
