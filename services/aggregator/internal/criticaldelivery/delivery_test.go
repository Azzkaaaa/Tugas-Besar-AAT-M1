package criticaldelivery_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery/receiver"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

const token = "a-dedicated-critical-api-token"
const secret = "field-team-receiver-sender-secret"

type journal struct {
	mu       sync.Mutex
	event    storage.CriticalEvent
	fail     bool
	partial  bool
	deadline bool
}

func (j *journal) Pending(ctx context.Context, c storage.CriticalCursor, n int) (storage.CriticalPage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	until, ok := ctx.Deadline()
	j.deadline = ok && time.Until(until) <= 3*time.Second
	p := storage.CriticalPage{Events: []storage.CriticalEvent{}}
	if j.event.Receipt == nil {
		p.Events = append(p.Events, j.event)
	}
	if j.partial {
		p.UnavailableUnits = []string{"critical-03"}
		p.Next = &c
	}
	return p, nil
}
func (j *journal) List(ctx context.Context, c storage.CriticalCursor, n int) (storage.CriticalPage, error) {
	return j.Pending(ctx, c, n)
}
func (j *journal) Version(context.Context) ([]int64, error) { return []int64{1}, nil }
func (j *journal) ListSnapshot(ctx context.Context, c storage.CriticalCursor, n int, _ []int64) (storage.CriticalPage, error) {
	return j.List(ctx, c, n)
}
func (j *journal) Get(_ context.Context, h, id string) (*storage.CriticalEvent, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if id != j.event.EventID || h != j.event.HazardID {
		return nil, storage.ErrCriticalNotFound
	}
	e := j.event
	return &e, nil
}
func (j *journal) RecordReceipt(_ context.Context, r storage.CriticalReceipt) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.fail {
		return errors.New("store unavailable")
	}
	if j.event.Receipt == nil {
		j.event.Receipt = &r
	}
	return nil
}
func event(t *testing.T) storage.CriticalEvent {
	t.Helper()
	h := &domain.HazardRecord{HazardID: domain.HazardID(domain.SourceCoastal, "obs"), Source: domain.SourceCoastal, SourceRefID: "obs", HazardType: domain.HazardTypeCoastal, Severity: domain.SeverityAwas, Revision: 1, Attributes: map[string]any{"critical": true}}
	e, err := storage.NewCriticalEvent(h, nil)
	if err != nil {
		t.Fatal(err)
	}
	e.CreatedAt = time.Now().UTC()
	return *e
}

func TestAPIRequiresReceiverProofAndDurableStorage(t *testing.T) {
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	j := &journal{event: event(t)}
	api := criticaldelivery.API{Journal: j, Token: token, ReceiverID: "field-team", PublicKey: public}.Handler(nil)
	request := func(method, path, auth string, v any) int {
		t.Helper()
		var body []byte
		if v != nil {
			body, _ = json.Marshal(v)
		}
		q := httptest.NewRequest(method, path, bytes.NewReader(body))
		q.Header.Set("Authorization", "Bearer "+auth)
		w := httptest.NewRecorder()
		api.ServeHTTP(w, q)
		return w.Code
	}
	if request("GET", "/internal/critical/pending", "audit-token", nil) != 401 {
		t.Fatal("audit/media credential gained access")
	}
	if request("GET", "/internal/critical/pending?limit=0", token, nil) != 400 || request("GET", "/internal/critical/pending?cursor=garbage", token, nil) != 400 {
		t.Fatal("invalid pagination accepted")
	}
	if request("GET", "/internal/critical/pending", token, nil) != 200 || !j.deadline {
		t.Fatal("missing bounded authorized read")
	}
	r := storage.CriticalReceipt{EventID: j.event.EventID, HazardID: j.event.HazardID, ReceiverID: "field-team", PayloadSHA256: j.event.PayloadSHA256, ReceivedAt: time.Now().UTC()}
	if request("POST", "/internal/critical/receipts", token, r) != 403 {
		t.Fatal("provider acceptance counted as receipt")
	}
	signed, err := criticaldelivery.SignReceipt(private, r)
	if err != nil {
		t.Fatal(err)
	}
	wrong := signed
	wrong.PayloadSHA256 = strings.Repeat("0", 64)
	wrong, _ = criticaldelivery.SignReceipt(private, wrong)
	if request("POST", "/internal/critical/receipts", token, wrong) != 409 {
		t.Fatal("wrong payload receipt accepted")
	}
	_, foreign, _ := ed25519.GenerateKey(rand.Reader)
	forged, _ := criticaldelivery.SignReceipt(foreign, r)
	if request("POST", "/internal/critical/receipts", token, forged) != 403 {
		t.Fatal("foreign receiver proof accepted")
	}
	j.fail = true
	if request("POST", "/internal/critical/receipts", token, signed) != 503 || j.event.Receipt != nil {
		t.Fatal("undurable receipt acknowledged")
	}
	j.fail = false
	if request("POST", "/internal/critical/receipts", token, signed) != 200 || j.event.Receipt == nil {
		t.Fatal("valid receipt rejected")
	}
}

func TestRestartedWorkerAndReceiverRecoverAnUnacknowledgedReceipt(t *testing.T) {
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	j := &journal{event: event(t)}
	var dropReceipt atomic.Bool
	dropReceipt.Store(true)
	api := criticaldelivery.API{Journal: j, Token: token, ReceiverID: "field-team", PublicKey: public}.Handler(nil)
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if dropReceipt.Load() && q.URL.Path == "/internal/critical/receipts" {
			w.WriteHeader(503)
			return
		}
		api.ServeHTTP(w, q)
	}))
	defer apiServer.Close()
	path := filepath.Join(t.TempDir(), "receipts.db")
	r, err := receiver.Open(path, "field-team", secret, private)
	if err != nil {
		t.Fatal(err)
	}
	firstTime := time.Now().UTC().Truncate(time.Millisecond)
	r.Now = func() time.Time { return firstTime }
	receiverServer := httptest.NewServer(r.Handler())
	worker := func(endpoint string) *criticaldelivery.Worker {
		return &criticaldelivery.Worker{APIURL: apiServer.URL, Token: token, ReceiverURL: endpoint + "/notifications", ReceiverID: "field-team", ReceiverSecret: secret, PublicKey: public, Concurrency: 2, Client: criticaldelivery.NewHTTPClient()}
	}
	if _, err := worker(receiverServer.URL).Cycle(context.Background(), ""); err == nil || j.event.Receipt != nil {
		t.Fatal("lost canonical acknowledgement was not retried", err)
	}
	receiverServer.Close()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// Both components lose their process state. Only the receiver's committed
	// SQLite receipt and the canonical pending event survive.
	r, err = receiver.Open(path, "field-team", secret, private)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.Now = func() time.Time { return firstTime.Add(time.Hour) }
	receiverServer = httptest.NewServer(r.Handler())
	defer receiverServer.Close()
	dropReceipt.Store(false)
	var group sync.WaitGroup
	errorsFound := make(chan error, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := worker(receiverServer.URL).Cycle(context.Background(), "")
			errorsFound <- err
		}()
	}
	group.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	if j.event.Receipt == nil || !j.event.Receipt.ReceivedAt.Equal(firstTime) {
		t.Fatal("first receiver receipt lost/replaced", j.event.Receipt)
	}
	var exported bytes.Buffer
	if err := r.Export(context.Background(), &exported); err != nil {
		t.Fatal(err)
	}
	if strings.Count(exported.String(), "\n") != 1 {
		t.Fatal("duplicate receiver processing", exported.String())
	}
	if err := criticaldelivery.VerifyReceipt(public, "field-team", *j.event.Receipt); err != nil {
		t.Fatal(err)
	}
}

func TestReceiverRejectsBadSenderAndConflictingReplay(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	r, err := receiver.Open(filepath.Join(t.TempDir(), "receipts.db"), "field-team", secret, key)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	e := event(t)
	post := func(body []byte, signature string) int {
		q := httptest.NewRequest("POST", "/notifications", bytes.NewReader(body))
		q.Header.Set("X-BNPB-Signature", signature)
		w := httptest.NewRecorder()
		r.Handler().ServeHTTP(w, q)
		return w.Code
	}
	if post(e.Payload, "invalid") != 401 {
		t.Fatal("unauthenticated notification accepted")
	}
	if post(e.Payload, criticaldelivery.RequestMAC(secret, e.Payload)) != 200 {
		t.Fatal("valid notification rejected")
	}
	changed := bytes.Replace(e.Payload, []byte(`"critical":true`), []byte(`"critical":true,"extra":1`), 1)
	if post(changed, criticaldelivery.RequestMAC(secret, changed)) != 409 {
		t.Fatal("event ID reused with changed bytes")
	}
	var out bytes.Buffer
	if err := r.Export(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "\n") != 1 {
		t.Fatal("replay altered durable receipts")
	}
	if response := httptest.NewRecorder(); response != nil {
		q := httptest.NewRequest("GET", "/receipts", nil)
		r.Handler().ServeHTTP(response, q)
		if response.Code != 404 {
			t.Fatal("unauthenticated receipt export exposed")
		}
	}
}

func TestJournalExportRequiresCompleteSnapshotGenerations(t *testing.T) {
	public, _, _ := ed25519.GenerateKey(rand.Reader)
	j := &journal{event: event(t)}
	j.event.Generation = 1
	server := httptest.NewServer(criticaldelivery.API{Journal: j, Token: token, ReceiverID: "field-team", PublicKey: public}.Handler(nil))
	defer server.Close()
	w := &criticaldelivery.Worker{APIURL: server.URL, Token: token, Client: criticaldelivery.NewHTTPClient()}
	var output bytes.Buffer
	if err := w.ExportJournal(context.Background(), &output); err != nil || !strings.Contains(output.String(), `"complete":true`) {
		t.Fatal("complete snapshot export failed", output.String(), err)
	}
	j.mu.Lock()
	j.partial = true
	j.mu.Unlock()
	output.Reset()
	if err := w.ExportJournal(context.Background(), &output); err == nil || strings.Contains(output.String(), `"complete":true`) {
		t.Fatal("partial snapshot claimed completion", err)
	}
	j.mu.Lock()
	j.partial = false
	j.event.Receipt = &storage.CriticalReceipt{}
	j.mu.Unlock()
	output.Reset()
	if err := w.ExportJournal(context.Background(), &output); err == nil {
		t.Fatal("missing generation claimed completion")
	}
}
