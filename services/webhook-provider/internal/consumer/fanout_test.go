package consumer

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

func TestFanoutSharesPayloadPerView(t *testing.T) {
	var event eventv1.HazardEventPublished
	if err := proto.Unmarshal(sampleEvent(t, 1), &event); err != nil {
		t.Fatal(err)
	}
	p := &eventPayloads{event: &event}
	var bodies [32][]byte
	var wg sync.WaitGroup
	for i := range bodies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			view := commonv1.View_VIEW_SUMMARY
			if i%2 == 0 {
				view = commonv1.View_VIEW_FULL
			}
			body, err := p.get(view)
			if err != nil {
				t.Error(err)
			}
			bodies[i] = body
		}()
	}
	wg.Wait()
	for i := 2; i < len(bodies); i++ {
		if &bodies[i][0] != &bodies[i%2][0] {
			t.Fatal("payload copied/rebuilt for a recipient")
		}
	}
	if bytes.Equal(bodies[0], bodies[1]) || !bytes.Contains(bodies[0], []byte("source_ref_id")) || bytes.Contains(bodies[1], []byte("source_ref_id")) {
		t.Fatal("FULL/SUMMARY projection mixed")
	}
}

func fanoutFixture(t *testing.T, db store.Store) *Consumer {
	t.Helper()
	cfg := &config.Config{HTTPTimeoutMS: 3 * time.Second, MaxDeliveryRetries: 1}
	claims := dedup.NewMemory(time.Hour)
	t.Cleanup(func() { claims.Close() })
	return New(cfg, db, claims, dispatcher.New(cfg.HTTPTimeoutMS, cfg.MaxDeliveryRetries))
}

func createFanoutSubscriptions(t *testing.T, db store.Store, target string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		view := commonv1.View_VIEW_SUMMARY
		if i%2 == 0 {
			view = commonv1.View_VIEW_FULL
		}
		err := db.CreateSubscription(context.Background(), &store.Subscription{WebhookID: fmt.Sprintf("sub-%03d", i), URL: target + "/" + strconv.Itoa(i), Secret: "fixture-key", View: int32(view), IsActive: true})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFanoutBoundsWorkersAndExcludesLateRegistrations(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenSQLite(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var active, peak, healthy atomic.Int32
	var mu sync.Mutex
	seen := map[string]int{}
	started, progress, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		mac := hmac.New(sha256.New, []byte("fixture-key"))
		mac.Write(body)
		if r.Header.Get("X-BNPB-Signature") != "sha256="+hex.EncodeToString(mac.Sum(nil)) {
			t.Error("incorrect HMAC")
		}
		id, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
		if err != nil {
			t.Error(err)
			return
		}
		if bytes.Contains(body, []byte("source_ref_id")) != (id%2 == 0) {
			t.Error("wrong FULL/SUMMARY projection")
		}
		mu.Lock()
		seen[r.URL.Path]++
		mu.Unlock()
		if id == 0 {
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
			}
		} else {
			if healthy.Add(1) == 10 {
				close(progress)
			}
			time.Sleep(3 * time.Millisecond)
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	createFanoutSubscriptions(t, db, server.URL, 80)
	c := fanoutFixture(t, db)
	ack := &mockAcknowledger{}
	done := make(chan struct{})
	go func() { c.handleMessage(ctx, amqp.Delivery{Acknowledger: ack, Body: sampleEvent(t, 1)}); close(done) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("slow receiver did not start")
	}
	if err := db.CreateSubscription(ctx, &store.Subscription{WebhookID: "000-late", URL: server.URL + "/late", Secret: "fixture-key", IsActive: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-progress:
	case <-time.After(2 * time.Second):
		t.Fatal("slow receiver blocked healthy recipients")
	}
	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fanout did not finish")
	}
	if ack.ackCount != 1 || ack.nackCount != 0 || peak.Load() > fanoutWorkers || peak.Load() < 2 {
		t.Fatalf("ack=%+v peak=%d", ack, peak.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 80 || seen["/late"] != 0 {
		t.Fatalf("walk included late registration or missed rows: %d", len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("duplicate %s: %d", id, count)
		}
	}
}

type failSecondBatch struct {
	store.Store
	calls int
}

func (s *failSecondBatch) ListActiveSubscriptions(ctx context.Context, after, through int64) ([]*store.Subscription, int64, error) {
	s.calls++
	if s.calls == 2 {
		return nil, after, fmt.Errorf("fixture second-page failure")
	}
	return s.Store.ListActiveSubscriptions(ctx, after, through)
}

func TestFanoutPageFailureRequeuesWithoutRepeatingSuccesses(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenSQLite(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var mu sync.Mutex
	seen := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		mu.Lock()
		seen[r.URL.Path]++
		mu.Unlock()
		w.WriteHeader(204)
	}))
	defer server.Close()
	createFanoutSubscriptions(t, db, server.URL, 80)
	c := fanoutFixture(t, &failSecondBatch{Store: db})
	body := sampleEvent(t, 1)
	first := &mockAcknowledger{}
	c.handleMessage(ctx, amqp.Delivery{Acknowledger: first, Body: body})
	if first.nackCount != 1 || !first.nackedRequeue {
		t.Fatalf("first outcome=%+v", first)
	}
	if len(seen) != store.ActiveBatchRows {
		t.Fatalf("first page delivered %d", len(seen))
	}
	second := &mockAcknowledger{}
	c.handleMessage(ctx, amqp.Delivery{Acknowledger: second, Body: body, Redelivered: true})
	if second.ackCount != 1 || second.nackCount != 0 || len(seen) != 80 {
		t.Fatalf("replay outcome=%+v deliveries=%d", second, len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("replay repeated %s: %d", id, count)
		}
	}
	for i := 0; i < 80; i++ {
		logs, _, err := db.ListDeliveries(ctx, fmt.Sprintf("sub-%03d", i), "", 2)
		if err != nil || len(logs) != 1 || logs[0].Status != "SUCCESS" {
			t.Fatalf("logs for %d: %v %v", i, logs, err)
		}
	}
}

func TestFanoutShutdownJoinsWorkersAndReleasesClaims(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := store.OpenSQLite(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	started := make(chan struct{}, fanoutWorkers)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		calls.Add(1)
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	createFanoutSubscriptions(t, db, server.URL, 80)
	c := fanoutFixture(t, db)
	claims := &strictClaimer{Claimer: c.claims}
	c.claims = claims
	body := sampleEvent(t, 1)
	ack := &mockAcknowledger{}
	done := make(chan struct{})
	go func() {
		c.handleMessage(ctx, amqp.Delivery{Acknowledger: ack, Body: body, Redelivered: true})
		close(done)
	}()
	for range fanoutWorkers {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("worker did not start")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown left workers running")
	}
	if ack.nackCount != 1 || !ack.nackedRequeue || ack.ackCount != 0 || claims.releases != fanoutWorkers || calls.Load() != fanoutWorkers {
		t.Fatalf("shutdown outcome: ack=%+v releases=%d calls=%d", ack, claims.releases, calls.Load())
	}
	for i := 0; i < 80; i++ {
		id := fmt.Sprintf("sub-%03d", i)
		logs, _, err := db.ListDeliveries(context.Background(), id, "", 1)
		if err != nil || len(logs) != 0 {
			t.Fatalf("interrupted attempt was logged: %v %v", logs, err)
		}
		_, result, err := c.claims.Claim(context.Background(), "evt-123", id, time.Minute)
		if err != nil || result != dedup.Claimed {
			t.Fatalf("claim retained after join: %s %v %v", id, result, err)
		}
	}
}
