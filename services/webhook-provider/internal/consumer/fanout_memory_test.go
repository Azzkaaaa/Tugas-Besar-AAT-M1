package consumer

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/dedup"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/dispatcher"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/store"
	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const fixtureDetectionKeys = 200000

func detectionKey(i int) string { return fmt.Sprintf("VIIRS-%06d-%08d", i, i*17) }

func largeFanoutEvent(t *testing.T) []byte {
	t.Helper()
	keys := make([]*structpb.Value, fixtureDetectionKeys)
	for i := range keys {
		keys[i] = structpb.NewStringValue(detectionKey(i))
	}
	var event eventv1.HazardEventPublished
	if err := proto.Unmarshal(sampleEvent(t, 1), &event); err != nil {
		t.Fatal(err)
	}
	event.Source = commonv1.Source_SOURCE_SATELITE
	event.Payload.Source = event.Source
	event.Payload.HazardType = commonv1.HazardType_HAZARD_TYPE_WILDFIRE
	event.Payload.Attributes = &structpb.Struct{Fields: map[string]*structpb.Value{
		"detection_keys": structpb.NewListValue(&structpb.ListValue{Values: keys}),
		"critical":       structpb.NewBoolValue(true),
	}}
	body, err := proto.Marshal(&event)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// Parse the receiver body as a stream: the fixture must not manufacture a second
// huge DOM that would hide the production consumer's own memory use.
func inspectFanoutJSON(dec *json.Decoder, full bool) error {
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return fmt.Errorf("invalid envelope: %v", err)
	}
	sawData, sawKeys := false, false
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return err
		}
		if key != "data" {
			var small json.RawMessage
			if err := dec.Decode(&small); err != nil {
				return err
			}
			continue
		}
		sawData = true
		if token, err := dec.Token(); err != nil || token != json.Delim('{') {
			return fmt.Errorf("invalid data object")
		}
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			if !full && (key == "attributes" || key == "source_ref_id" || key == "latitude" || key == "longitude") {
				return fmt.Errorf("SUMMARY exposed %s", key)
			}
			if key != "attributes" {
				var small json.RawMessage
				if err := dec.Decode(&small); err != nil {
					return err
				}
				continue
			}
			if token, err := dec.Token(); err != nil || token != json.Delim('{') {
				return fmt.Errorf("invalid attributes")
			}
			for dec.More() {
				key, err := dec.Token()
				if err != nil {
					return err
				}
				if key != "detection_keys" {
					var small json.RawMessage
					if err := dec.Decode(&small); err != nil {
						return err
					}
					continue
				}
				sawKeys = true
				if token, err := dec.Token(); err != nil || token != json.Delim('[') {
					return fmt.Errorf("invalid detection list")
				}
				count := 0
				for dec.More() {
					value, err := dec.Token()
					if err != nil {
						return err
					}
					if value != detectionKey(count) {
						return fmt.Errorf("detection key %d differs", count)
					}
					count++
				}
				if count != fixtureDetectionKeys {
					return fmt.Errorf("keys=%d want=%d", count, fixtureDetectionKeys)
				}
				if _, err := dec.Token(); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("unexpected trailing content: %v", err)
	}
	if !sawData || sawKeys != full {
		return fmt.Errorf("projection missing: data=%v keys=%v full=%v", sawData, sawKeys, full)
	}
	return nil
}

type countedWalkStore struct {
	store.Store
	heads atomic.Int32
}

func (s *countedWalkStore) ActiveSubscriptionHead(ctx context.Context) (int64, error) {
	s.heads.Add(1)
	return s.Store.ActiveSubscriptionHead(ctx)
}

func TestWebhookFanoutMemory(t *testing.T) {
	dsn, brokerURL, redisURL := os.Getenv("WEBHOOK_TEST_POSTGRES_DSN"), os.Getenv("WEBHOOK_TEST_AMQP_URL"), os.Getenv("WEBHOOK_TEST_REDIS_URL")
	if dsn == "" || brokerURL == "" || redisURL == "" {
		t.Skip("requires named disposable PostgreSQL/RabbitMQ/Redis fixtures")
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Database != "webhook_m2_fixture" {
		t.Fatal("refusing non-fixture database")
	}
	// Match cmd/main.go's default, including native test binaries without main.
	previous := debug.SetMemoryLimit(160 << 20)
	defer debug.SetMemoryLimit(previous)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	db, err := store.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if head, err := db.ActiveSubscriptionHead(ctx); err != nil || head != 0 {
		t.Fatalf("requires empty fixture: head=%d err=%v", head, err)
	}
	claims, err := dedup.NewRedis(ctx, redisURL, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer claims.Close()
	var active, peak, healthy atomic.Int32
	var mu sync.Mutex
	seen := map[int]int{}
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
		}
		id, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		mac := hmac.New(sha256.New, []byte("fixture-key"))
		if err := inspectFanoutJSON(json.NewDecoder(io.TeeReader(r.Body, mac)), id%2 == 0); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if r.Header.Get("X-BNPB-Signature") != "sha256="+hex.EncodeToString(mac.Sum(nil)) {
			t.Error("HMAC does not cover exact complete bytes")
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		seen[id]++
		attempt := seen[id]
		mu.Unlock()
		if id == 0 {
			select {
			case <-release:
			case <-ctx.Done():
			case <-r.Context().Done():
			}
		} else if healthy.Add(1) == 10 {
			unblock()
		}
		if id == 1 && attempt == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	createFanoutSubscriptions(t, db, server.URL, 80)
	tracked := &countedWalkStore{Store: db}
	cfg := &config.Config{AMQPURL: brokerURL, RabbitMQExchange: "m2.fixture.webhook", ConsumerQueue: "m2.fixture.webhook", ConsumerDLQ: "m2.fixture.webhook.dlq", HTTPTimeoutMS: 10 * time.Second, MaxDeliveryRetries: 2, ShutdownGrace: 5 * time.Second}
	c := New(cfg, tracked, claims, dispatcher.New(cfg.HTTPTimeoutMS, cfg.MaxDeliveryRetries))
	conn, ch, err := c.connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	defer ch.Close()
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 2))
	body := largeFanoutEvent(t)
	wireBytes := len(body)
	runtime.GC()
	work, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan struct{})
	go func() { defer close(done); c.Start(work) }()
	// Replay the exact same immutable publication through the real broker.
	for range 2 {
		if err := ch.PublishWithContext(ctx, cfg.RabbitMQExchange, "", true, false, amqp.Publishing{Body: body, MessageId: "evt-123", ContentType: "application/protobuf", DeliveryMode: amqp.Persistent}); err != nil {
			t.Fatal(err)
		}
		select {
		case confirm := <-confirms:
			if !confirm.Ack {
				t.Fatal("broker rejected fixture")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	body = nil
	for tracked.heads.Load() < 2 {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	stop()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("consumer did not drain")
	}
	queue, err := ch.QueueInspect(cfg.ConsumerQueue)
	if err != nil {
		t.Fatal(err)
	}
	dlq, err := ch.QueueInspect(cfg.ConsumerDLQ)
	if err != nil {
		t.Fatal(err)
	}
	if queue.Messages != 0 || dlq.Messages != 0 {
		t.Fatalf("unacknowledged/dead-lettered messages: %d/%d", queue.Messages, dlq.Messages)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 80 || peak.Load() > fanoutWorkers || peak.Load() < 2 {
		t.Fatalf("recipients=%d peak=%d", len(seen), peak.Load())
	}
	for i := 0; i < 80; i++ {
		want := 1
		if i == 1 {
			want = 2
		}
		if seen[i] != want {
			t.Fatalf("recipient %d attempts=%d want=%d", i, seen[i], want)
		}
		logs, _, err := db.ListDeliveries(ctx, fmt.Sprintf("sub-%03d", i), "", 2)
		if err != nil || len(logs) != 1 || logs[0].Status != "SUCCESS" {
			t.Fatalf("logs %d=%v: %v", i, logs, err)
		}
	}
	t.Logf("native fixture: keys=%d wire_bytes=%d recipients=80 full=40 summary=40 attempts=81 peak_workers=%d broker_replays=1 logs=80 queue=0 dlq=0", fixtureDetectionKeys, wireBytes, peak.Load())
}
