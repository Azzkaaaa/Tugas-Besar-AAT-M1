package reconcile

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type fixture struct {
	public                      ed25519.PublicKey
	private                     ed25519.PrivateKey
	start                       time.Time
	expected, journal, receipts bytes.Buffer
	events                      []storage.CriticalEvent
	count                       int64
}

func newFixture(t *testing.T, n int) *fixture {
	t.Helper()
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	f := &fixture{public: public, private: private, start: time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)}
	for i := range n {
		f.add(t, string(rune('a'+i)), 1)
	}
	return f
}
func (f *fixture) add(t *testing.T, ref string, revision int) {
	t.Helper()
	yes := true
	if revision == 1 {
		if err := json.NewEncoder(&f.expected).Encode(Expected{ObservationID: ref, Critical: &yes, Valid: &yes, EmittedAt: f.start}); err != nil {
			t.Fatal(err)
		}
	}
	h := &domain.HazardRecord{HazardID: domain.HazardID(domain.SourceCoastal, ref), Source: domain.SourceCoastal, SourceRefID: ref, HazardType: domain.HazardTypeCoastal, Severity: domain.SeverityAwas, Revision: revision, Attributes: map[string]any{"critical": true}}
	e, err := storage.NewCriticalEvent(h, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.count++
	e.Generation = f.count
	f.events = append(f.events, *e)
	if err := json.NewEncoder(&f.journal).Encode(e); err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) receipt(t *testing.T, index int, latency time.Duration) {
	t.Helper()
	e := f.events[index]
	r, err := criticaldelivery.SignReceipt(f.private, storage.CriticalReceipt{EventID: e.EventID, HazardID: e.HazardID, ReceiverID: "field-team", PayloadSHA256: e.PayloadSHA256, ReceivedAt: f.start.Add(latency)})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(&f.receipts).Encode(r); err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) run(t *testing.T) (Report, error) {
	t.Helper()
	var journal bytes.Buffer
	journal.Write(f.journal.Bytes())
	_ = json.NewEncoder(&journal).Encode(criticaldelivery.Manifest{Kind: "critical-journal-manifest", Complete: true, Count: f.count, Versions: []int64{f.count}, CapturedAt: f.start.Add(time.Minute)})
	return Run(context.Background(), bytes.NewReader(f.expected.Bytes()), bytes.NewReader(f.receipts.Bytes()), Options{PublicKey: f.public, ReceiverID: "field-team", Cutoff: f.start.Add(time.Minute), Latency: 5 * time.Second, TimelyTarget: .95, TempDir: t.TempDir(), Journal: &journal})
}

func TestReconciliationCountsMissingLateAndBudgetExactly(t *testing.T) {
	f := newFixture(t, 3)
	f.receipt(t, 0, time.Second)
	f.receipt(t, 1, 6*time.Second)
	r, err := f.run(t)
	if err != nil {
		t.Fatal(err)
	}
	if r.Expected != 3 || r.Received != 2 || r.Missing != 1 || r.Late != 1 || r.MissingCanonicalRevisions != 1 || r.TargetMet || r.P95Seconds == nil || *r.P95Seconds != 6 {
		t.Fatalf("incorrect receiver reconciliation: %+v", r)
	}
	if math.Abs(r.BudgetConsumedFraction-2/.15) > 1e-9 || r.LossBudget != 0 || !r.WindowComplete {
		t.Fatal("incorrect consumed error budget", r)
	}
}
func TestAllCriticalRevisionsMustHaveReceipts(t *testing.T) {
	f := newFixture(t, 1)
	f.add(t, "a", 2)
	f.receipt(t, 0, time.Second)
	r, err := f.run(t)
	if err != nil {
		t.Fatal(err)
	}
	if r.Missing != 0 || r.MissingCanonicalRevisions != 1 || r.TargetMet {
		t.Fatal("one received observation hid its missing revision", r)
	}
	f.receipt(t, 1, 2*time.Second)
	f.receipt(t, 0, time.Second)
	r, err = f.run(t)
	if err != nil {
		t.Fatal(err)
	}
	if !r.TargetMet || r.CanonicalRevisions != 2 || r.DuplicateReceipts != 1 {
		t.Fatal("complete deduplicated receipts rejected", r)
	}
}
func TestReconciliationRejectsUnverifiableAndPartialEvidence(t *testing.T) {
	f := newFixture(t, 1)
	f.receipts.WriteString("{\"event_id\":\"provider-accepted\"}\n")
	r, err := f.run(t)
	if err != nil {
		t.Fatal(err)
	}
	if r.InvalidReceipts != 1 || r.TargetMet {
		t.Fatal("provider acceptance counted as receiver proof", r)
	}
	options := Options{PublicKey: f.public, ReceiverID: "field-team", Cutoff: f.start.Add(time.Minute), Latency: 5 * time.Second, TimelyTarget: .95, TempDir: t.TempDir(), Journal: bytes.NewReader(f.journal.Bytes())}
	if _, err = Run(context.Background(), bytes.NewReader(f.expected.Bytes()), bytes.NewReader(f.receipts.Bytes()), options); err == nil {
		t.Fatal("missing snapshot manifest accepted")
	}
	options.Journal = strings.NewReader(`{"kind":"critical-journal-manifest","complete":true,"versions":[2],"count":2}` + "\n")
	if _, err = Run(context.Background(), bytes.NewReader(f.expected.Bytes()), bytes.NewReader(nil), options); err == nil {
		t.Fatal("missing committed generations accepted")
	}
	options.Journal = strings.NewReader(`{"kind":"critical-journal-manifest","complete":true,"versions":[0],"count":0}` + "\n")
	if _, err = Run(context.Background(), bytes.NewReader(nil), bytes.NewReader(nil), options); err == nil {
		t.Fatal("empty source evidence declared success")
	}
}
func TestClockAnomaliesAndExcludedInvalidInputsStayVisible(t *testing.T) {
	f := newFixture(t, 1)
	f.receipt(t, 0, -time.Second)
	yes, no := true, false
	_ = json.NewEncoder(&f.expected).Encode(Expected{ObservationID: "invalid", Critical: &yes, Valid: &no})
	r, err := f.run(t)
	if err != nil {
		t.Fatal(err)
	}
	if r.ClockAnomalies != 1 || r.ExcludedInvalid != 1 || r.P95Seconds != nil || r.TargetMet {
		t.Fatal("invalid/clock evidence was hidden", r)
	}
}
