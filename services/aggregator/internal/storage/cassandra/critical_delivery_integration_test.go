//go:build integration

package cassandra

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery/receiver"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery/reconcile"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

// The integration binary is its own small receiver subprocess. The parent
// forcibly kills it after durable receipt, without graceful DB shutdown.
func TestCriticalReceiverProcessHelper(t *testing.T) {
	if os.Getenv("M2_CRITICAL_RECEIVER_HELPER") != "1" {
		return
	}
	key, err := base64.StdEncoding.DecodeString(os.Getenv("M2_HELPER_PRIVATE_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := receiver.Open(os.Getenv("M2_HELPER_DB"), "field-team", "integration-receiver-secret", ed25519.PrivateKey(key))
	if err != nil {
		t.Fatal(err)
	}
	server := http.Server{Addr: os.Getenv("M2_HELPER_ADDRESS"), Handler: r.Handler(), ReadHeaderTimeout: time.Second}
	if err := server.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
}

func TestCriticalDeliveryRecoversAfterReceiverDeathAndDownstreamErasure(t *testing.T) {
	db, err := openTest(t, "agg_test_delivery_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()
	h := &domain.HazardRecord{Source: domain.SourceCoastal, SourceRefID: "critical-to-field-team", HazardType: domain.HazardTypeCoastal, Severity: domain.SeverityAwas, OccurredAt: time.Now().UTC(), Attributes: map[string]any{"critical": true, "wave_height_m": 2.0}}
	for i := range 2 {
		h.Attributes["wave_height_m"] = float64(2 + i)
		if _, _, err := db.Backend().Hazards.ApplyRevision(ctx, h, revEvent); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"outbox_events", "outbox_pending"} {
		if err := db.TruncateForTest(table); err != nil {
			t.Fatal(err)
		}
	}
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	api := criticaldelivery.API{Journal: db.Backend().Critical, Token: "canonical-critical-test-token", ReceiverID: "field-team", PublicKey: public}.Handler(nil)
	var failAcknowledgement atomic.Bool
	failAcknowledgement.Store(true)
	first := map[string]time.Time{}
	var lock sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if failAcknowledgement.Load() && q.URL.Path == "/internal/critical/receipts" {
			var receipt storage.CriticalReceipt
			if json.NewDecoder(q.Body).Decode(&receipt) != nil {
				t.Error("invalid worker receipt")
			}
			lock.Lock()
			first[receipt.EventID] = receipt.ReceivedAt
			lock.Unlock()
			w.WriteHeader(503)
			return
		}
		api.ServeHTTP(w, q)
	}))
	defer server.Close()
	database := filepath.Join(t.TempDir(), "field-team.db")
	startReceiver := func() (string, func()) {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		_ = listener.Close()
		command := exec.Command(os.Args[0], "-test.run=^TestCriticalReceiverProcessHelper$", "-test.timeout=2m")
		command.Env = append(os.Environ(), "M2_CRITICAL_RECEIVER_HELPER=1", "M2_HELPER_DB="+database, "M2_HELPER_ADDRESS="+address, "M2_HELPER_PRIVATE_KEY="+base64.StdEncoding.EncodeToString(private), "GOMEMLIMIT=64MiB", "GOMAXPROCS=1")
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		var once sync.Once
		stop := func() {
			once.Do(func() {
				_ = command.Process.Kill()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("receiver process did not terminate")
				}
			})
		}
		t.Cleanup(stop)
		client := http.Client{Timeout: 200 * time.Millisecond}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			response, err := client.Get("http://" + address + "/healthz")
			if err == nil {
				response.Body.Close()
				if response.StatusCode == 204 {
					return "http://" + address + "/notifications", stop
				}
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatal("receiver subprocess did not become ready")
		return "", stop
	}
	endpoint, kill := startReceiver()
	worker := func(url string) *criticaldelivery.Worker {
		return &criticaldelivery.Worker{APIURL: server.URL, Token: "canonical-critical-test-token", ReceiverURL: url, ReceiverID: "field-team", ReceiverSecret: "integration-receiver-secret", PublicKey: public, Concurrency: 2, Client: criticaldelivery.NewHTTPClient()}
	}
	if _, err := worker(endpoint).Cycle(ctx, ""); err == nil {
		t.Fatal("lost canonical acknowledgement was hidden")
	}
	lock.Lock()
	recorded := len(first)
	lock.Unlock()
	if recorded != 2 {
		t.Fatal("receiver did not persist both revisions", recorded)
	}
	kill()
	// No broker or notification database is running. A new process and new
	// stateless worker must recover solely from canonical pending + receiver WAL.
	endpoint, _ = startReceiver()
	failAcknowledgement.Store(false)
	if _, err := worker(endpoint).Cycle(ctx, ""); err != nil {
		t.Fatal(err)
	}
	pending, err := db.Backend().Critical.Pending(ctx, storage.CriticalCursor{}, 100)
	if err != nil || len(pending.Events) != 0 {
		t.Fatal("critical delivery missing after recovery", pending, err)
	}
	history, err := db.Backend().Critical.List(ctx, storage.CriticalCursor{}, 100)
	if err != nil || len(history.Events) != 2 {
		t.Fatal(history, err)
	}
	for _, e := range history.Events {
		lock.Lock()
		firstTime := first[e.EventID]
		lock.Unlock()
		if e.Receipt == nil || !e.Receipt.ReceivedAt.Equal(firstTime) {
			t.Fatal("receiver death lost original durable receipt", e)
		}
		if err := criticaldelivery.VerifyReceipt(public, "field-team", *e.Receipt); err != nil {
			t.Fatal(err)
		}
	}
	// Close the source measurement window before capturing its immutable
	// generation snapshot. This timestamp is an actual fixture emission time,
	// rather than the accelerated observation time used by exam feed profiles.
	if wait := time.Until(h.OccurredAt.Add(5100 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	cutoff := time.Now().UTC()
	var journalEvidence bytes.Buffer
	if err := worker(endpoint).ExportJournal(ctx, &journalEvidence); err != nil {
		t.Fatal(err)
	}
	durableReceiver, err := receiver.Open(database, "field-team", "integration-receiver-secret", private)
	if err != nil {
		t.Fatal(err)
	}
	var receiptsEvidence bytes.Buffer
	if err := durableReceiver.Export(ctx, &receiptsEvidence); err != nil {
		t.Fatal(err)
	}
	_ = durableReceiver.Close()
	var expectedEvidence bytes.Buffer
	yes := true
	_ = json.NewEncoder(&expectedEvidence).Encode(reconcile.Expected{ObservationID: h.SourceRefID, Critical: &yes, Valid: &yes, EmittedAt: h.OccurredAt})
	report, err := reconcile.Run(ctx, bytes.NewReader(expectedEvidence.Bytes()), bytes.NewReader(receiptsEvidence.Bytes()), reconcile.Options{PublicKey: public, ReceiverID: "field-team", Cutoff: cutoff, Latency: 5 * time.Second, TimelyTarget: .95, TempDir: t.TempDir(), Journal: bytes.NewReader(journalEvidence.Bytes())})
	if err != nil || report.Missing != 0 || report.MissingCanonicalRevisions != 0 || report.PayloadMismatches != 0 || !report.WindowComplete {
		t.Fatal("source-to-receiver proof failed", report, err)
	}
	t.Logf("local source-to-receiver report: observations=%d revisions=%d missing=%d p95=%v target_met=%v", report.Expected, report.CanonicalRevisions, report.Missing, report.P95Seconds, report.TargetMet)
	if directory := os.Getenv("AGG_TEST_CRITICAL_EVIDENCE_DIR"); directory != "" {
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.MarshalIndent(report, "", "  ")
		for name, data := range map[string][]byte{"expected.jsonl": expectedEvidence.Bytes(), "receipts.jsonl": receiptsEvidence.Bytes(), "journal.jsonl": journalEvidence.Bytes(), "report.json": encoded, "receiver-public-key.txt": []byte(base64.StdEncoding.EncodeToString(public))} {
			if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
