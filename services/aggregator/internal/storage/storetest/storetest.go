// Package storetest is the behavioural contract of storage backends. Every backend runs Run
// against a real database in its integration tests.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

// Run runs the contract. newBackend must return a backend over an empty store, one per call.
func Run(t *testing.T, newBackend func(t *testing.T) *storage.Backend) {
	tests := []struct {
		name string
		fn   func(t *testing.T, b *storage.Backend)
	}{
		{"ApplyRevision/NewRecord", testApplyNew},
		{"ApplyRevision/Unchanged", testApplyUnchanged},
		{"ApplyRevision/Changed", testApplyChanged},
		{"ApplyRevision/NormalizesAttributes", testApplyNormalizes},
		{"ApplyRevision/EventErrorWritesNothing", testApplyEventError},
		{"Get/Missing", testGetMissing},
		{"List/OrderAndPaging", testListOrderAndPaging},
		{"List/Filters", testListFilters},
		{"List/TimeRange", testListTimeRange},
		{"List/MovedRecord", testListMovedRecord},
		{"List/InvalidToken", testListInvalidToken},
		{"HasData", testHasData},
		{"Sources", testSources},
		{"Outbox/ClaimAck", testOutboxClaimAck},
		{"Outbox/Lease", testOutboxLease},
		{"Outbox/RetryAndRelease", testOutboxRetryRelease},
		{"Outbox/DeadLetter", testOutboxDeadLetter},
		{"Outbox/ConcurrentClaimers", testOutboxConcurrent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newBackend(t)) })
	}
}

// base is a fixed, millisecond-precise time in the past so results do not depend on now.
var base = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

func hazard(source, ref string, occurred time.Time) *domain.HazardRecord {
	ht := domain.HazardTypeSeismic
	if source == domain.SourcePVMBG {
		ht = domain.HazardTypeVolcanic
	}
	return &domain.HazardRecord{
		Source:      source,
		SourceRefID: ref,
		HazardType:  ht,
		Severity:    domain.SeverityNormal,
		AreaName:    "Area " + ref,
		Latitude:    -6.2,
		Longitude:   106.8,
		OccurredAt:  occurred,
		Attributes:  map[string]any{"magnitude": 4.5},
	}
}

// eventRecorder builds outbox events and remembers them.
type eventRecorder struct {
	mu     sync.Mutex
	events []storage.OutboxEvent
}

func (r *eventRecorder) newEvent(saved *domain.HazardRecord) (storage.OutboxEvent, error) {
	ev := storage.OutboxEvent{
		EventID:      "pub-" + uuid.New().String(),
		HazardID:     saved.HazardID,
		Source:       saved.Source,
		Payload:      []byte("payload:" + saved.HazardID + ":" + saved.Severity),
		TraceContext: map[string]string{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
	}
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
	return ev, nil
}

func apply(t *testing.T, b *storage.Backend, rec *eventRecorder, h *domain.HazardRecord) (*domain.HazardRecord, bool) {
	t.Helper()
	saved, changed, err := b.Hazards.ApplyRevision(ctx(t), h, rec.newEvent)
	if err != nil {
		t.Fatalf("ApplyRevision(%s/%s): %v", h.Source, h.SourceRefID, err)
	}
	return saved, changed
}

func testApplyNew(t *testing.T, b *storage.Backend) {
	rec := &eventRecorder{}
	saved, changed := apply(t, b, rec, hazard(domain.SourceBMKG, "e1", base))
	if !changed {
		t.Fatal("new record: changed = false")
	}
	if saved.HazardID == "" || saved.IngestedAt.IsZero() {
		t.Fatalf("new record lacks id or ingested_at: %+v", saved)
	}
	if len(rec.events) != 1 || rec.events[0].HazardID != saved.HazardID {
		t.Fatalf("want one event for %s, got %+v", saved.HazardID, rec.events)
	}
	got, err := b.Hazards.Get(ctx(t), saved.HazardID)
	if err != nil || got == nil {
		t.Fatalf("Get: %v, %v", got, err)
	}
	if !got.OccurredAt.Equal(base) || got.AreaName != "Area e1" || got.Attributes["magnitude"] != 4.5 {
		t.Fatalf("Get returned %+v", got)
	}
	if !got.IngestedAt.Equal(saved.IngestedAt) {
		t.Fatalf("stored ingested_at %s, returned %s", got.IngestedAt, saved.IngestedAt)
	}
}

func testApplyUnchanged(t *testing.T, b *storage.Backend) {
	rec := &eventRecorder{}
	first, _ := apply(t, b, rec, hazard(domain.SourceBMKG, "e1", base))
	again, changed := apply(t, b, rec, hazard(domain.SourceBMKG, "e1", base))
	if changed {
		t.Fatal("identical revision reported changed")
	}
	if again.HazardID != first.HazardID || !again.IngestedAt.Equal(first.IngestedAt) {
		t.Fatalf("identity changed: %+v vs %+v", again, first)
	}
	if len(rec.events) != 1 {
		t.Fatalf("identical revision emitted an event: %d events", len(rec.events))
	}
}

func testApplyChanged(t *testing.T, b *storage.Backend) {
	rec := &eventRecorder{}
	first, _ := apply(t, b, rec, hazard(domain.SourceBMKG, "e1", base))
	rev := hazard(domain.SourceBMKG, "e1", base)
	rev.Severity = domain.SeveritySiaga
	rev.HazardID = "ignored"
	rev.IngestedAt = base.Add(time.Hour)
	saved, changed := apply(t, b, rec, rev)
	if !changed {
		t.Fatal("changed severity not detected")
	}
	if saved.HazardID != first.HazardID || !saved.IngestedAt.Equal(first.IngestedAt) {
		t.Fatalf("revision must keep hazard_id and ingested_at: %+v vs %+v", saved, first)
	}
	got, _ := b.Hazards.Get(ctx(t), first.HazardID)
	if got.Severity != domain.SeveritySiaga {
		t.Fatalf("stored severity %s", got.Severity)
	}
	if len(rec.events) != 2 {
		t.Fatalf("want 2 events, got %d", len(rec.events))
	}
}

func testApplyNormalizes(t *testing.T, b *storage.Backend) {
	rec := &eventRecorder{}
	h := hazard(domain.SourceBMKG, "e1", base)
	// Typed values a mapper produces (structpb rejects []map[string]any).
	h.Attributes = map[string]any{"count": 3, "warnings": []map[string]any{{"level": "Awas"}}}
	saved, _ := apply(t, b, rec, h)
	ws, ok := saved.Attributes["warnings"].([]any)
	if !ok || len(ws) != 1 || saved.Attributes["count"] != float64(3) {
		t.Fatalf("returned attributes not normalized: %#v", saved.Attributes)
	}
	if _, err := saved.ToProto(); err != nil {
		t.Fatalf("normalized record does not convert to proto: %v", err)
	}
	// The same typed input again is not a change.
	h2 := hazard(domain.SourceBMKG, "e1", base)
	h2.Attributes = map[string]any{"count": 3, "warnings": []map[string]any{{"level": "Awas"}}}
	if _, changed := apply(t, b, rec, h2); changed {
		t.Fatal("re-applying typed attributes reported a change")
	}
}

func testApplyEventError(t *testing.T, b *storage.Backend) {
	_, _, err := b.Hazards.ApplyRevision(ctx(t), hazard(domain.SourceBMKG, "e1", base),
		func(*domain.HazardRecord) (storage.OutboxEvent, error) {
			return storage.OutboxEvent{}, fmt.Errorf("boom")
		})
	if err == nil {
		t.Fatal("event builder error not returned")
	}
	page, err := b.Hazards.List(ctx(t), storage.HazardQuery{}, storage.PageRequest{Size: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 {
		t.Fatalf("hazard stored although its event failed: %+v", page.Items)
	}
	events, err := b.Outbox.ClaimDue(ctx(t), 10, time.Minute)
	if err != nil || len(events) != 0 {
		t.Fatalf("outbox not empty: %v, %v", events, err)
	}
}

func testGetMissing(t *testing.T, b *storage.Backend) {
	got, err := b.Hazards.Get(ctx(t), "hz-missing")
	if got != nil || err != nil {
		t.Fatalf("Get(missing) = %v, %v; want nil, nil", got, err)
	}
}

// seed stores n BMKG hazards spread over several hours (and so several buckets), including
// pairs with the same OccurredAt, and returns them in List order.
func seed(t *testing.T, b *storage.Backend, n int) []*domain.HazardRecord {
	rec := &eventRecorder{}
	var out []*domain.HazardRecord
	for i := 0; i < n; i++ {
		occurred := base.Add(time.Duration(i/2) * 37 * time.Minute) // pairs share a time
		source := domain.SourceBMKG
		if i%3 == 0 {
			source = domain.SourcePVMBG
		}
		h := hazard(source, fmt.Sprintf("r%03d", i), occurred)
		if i%4 == 0 {
			h.Severity = domain.SeverityWaspada
		}
		saved, _ := apply(t, b, rec, h)
		out = append(out, saved)
	}
	sortListOrder(out)
	return out
}

func sortListOrder(hs []*domain.HazardRecord) {
	sort.Slice(hs, func(i, j int) bool {
		if !hs[i].OccurredAt.Equal(hs[j].OccurredAt) {
			return hs[i].OccurredAt.After(hs[j].OccurredAt)
		}
		return hs[i].HazardID < hs[j].HazardID
	})
}

func listAll(t *testing.T, b *storage.Backend, q storage.HazardQuery, size int) []*domain.HazardRecord {
	t.Helper()
	var out []*domain.HazardRecord
	token := ""
	for pages := 0; ; pages++ {
		if pages > 1000 {
			t.Fatal("paging does not terminate")
		}
		page, err := b.Hazards.List(ctx(t), q, storage.PageRequest{Size: size, Token: token})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(page.Items) > size {
			t.Fatalf("page of %d items for size %d", len(page.Items), size)
		}
		out = append(out, page.Items...)
		if page.NextToken == "" {
			return out
		}
		token = page.NextToken
	}
}

func ids(hs []*domain.HazardRecord) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = h.HazardID
	}
	return out
}

func assertIDs(t *testing.T, got, want []*domain.HazardRecord) {
	t.Helper()
	g, w := ids(got), ids(want)
	if fmt.Sprint(g) != fmt.Sprint(w) {
		t.Fatalf("List returned\n  %v\nwant\n  %v", g, w)
	}
}

func filter(hs []*domain.HazardRecord, keep func(*domain.HazardRecord) bool) []*domain.HazardRecord {
	var out []*domain.HazardRecord
	for _, h := range hs {
		if keep(h) {
			out = append(out, h)
		}
	}
	return out
}

func testListOrderAndPaging(t *testing.T, b *storage.Backend) {
	all := seed(t, b, 25)
	for _, size := range []int{1, 2, 7, 25, 100} {
		assertIDs(t, listAll(t, b, storage.HazardQuery{}, size), all)
	}
}

func testListFilters(t *testing.T, b *storage.Backend) {
	all := seed(t, b, 25)
	cases := []storage.HazardQuery{
		{Source: domain.SourceBMKG},
		{Source: domain.SourcePVMBG},
		{HazardType: domain.HazardTypeVolcanic},
		{Severity: domain.SeverityWaspada},
		{Source: domain.SourceBMKG, Severity: domain.SeverityWaspada},
		{Source: domain.SourceBMKG, HazardType: domain.HazardTypeVolcanic}, // matches nothing
	}
	for _, q := range cases {
		want := filter(all, func(h *domain.HazardRecord) bool {
			return (q.Source == "" || h.Source == q.Source) &&
				(q.HazardType == "" || h.HazardType == q.HazardType) &&
				(q.Severity == "" || h.Severity == q.Severity)
		})
		assertIDs(t, listAll(t, b, q, 3), want)
	}
}

func testListTimeRange(t *testing.T, b *storage.Backend) {
	all := seed(t, b, 25)
	since := base.Add(74 * time.Minute)  // exactly the time of a pair: inclusive
	until := base.Add(222 * time.Minute) // exactly the time of a pair: exclusive
	want := filter(all, func(h *domain.HazardRecord) bool {
		return !h.OccurredAt.Before(since) && h.OccurredAt.Before(until)
	})
	if len(want) == 0 {
		t.Fatal("test data does not cover the range")
	}
	assertIDs(t, listAll(t, b, storage.HazardQuery{Since: since, Until: until}, 2), want)
	assertIDs(t, listAll(t, b, storage.HazardQuery{Since: since}, 4),
		filter(all, func(h *domain.HazardRecord) bool { return !h.OccurredAt.Before(since) }))
	assertIDs(t, listAll(t, b, storage.HazardQuery{Until: until}, 4),
		filter(all, func(h *domain.HazardRecord) bool { return h.OccurredAt.Before(until) }))
}

// A revision that changes OccurredAt moves the record; it must be listed once, at its new place.
func testListMovedRecord(t *testing.T, b *storage.Backend) {
	rec := &eventRecorder{}
	apply(t, b, rec, hazard(domain.SourceBMKG, "a", base))
	moved, _ := apply(t, b, rec, hazard(domain.SourceBMKG, "m", base.Add(time.Minute)))
	apply(t, b, rec, hazard(domain.SourceBMKG, "c", base.Add(3*time.Hour)))
	rev := hazard(domain.SourceBMKG, "m", base.Add(5*time.Hour))
	if _, changed := apply(t, b, rec, rev); !changed {
		t.Fatal("moved OccurredAt not detected")
	}
	got := listAll(t, b, storage.HazardQuery{}, 10)
	if len(got) != 3 || got[0].HazardID != moved.HazardID || !got[0].OccurredAt.Equal(rev.OccurredAt) {
		t.Fatalf("moved record not listed once at its new time: %v", ids(got))
	}
}

func testListInvalidToken(t *testing.T, b *storage.Backend) {
	_, err := b.Hazards.List(ctx(t), storage.HazardQuery{}, storage.PageRequest{Size: 5, Token: "%%not-a-token"})
	if err == nil {
		t.Fatal("invalid token accepted")
	}
	if !isInvalidToken(err) {
		t.Fatalf("want storage.ErrInvalidPageToken, got %v", err)
	}
}

func testHasData(t *testing.T, b *storage.Backend) {
	for _, src := range []string{domain.SourceBMKG, domain.SourcePVMBG} {
		if has, err := b.Hazards.HasData(ctx(t), src); err != nil || has {
			t.Fatalf("HasData(%s) on empty store = %v, %v", src, has, err)
		}
	}
	apply(t, b, &eventRecorder{}, hazard(domain.SourceBMKG, "e1", base))
	if has, err := b.Hazards.HasData(ctx(t), domain.SourceBMKG); err != nil || !has {
		t.Fatalf("HasData(BMKG) = %v, %v", has, err)
	}
	if has, err := b.Hazards.HasData(ctx(t), domain.SourcePVMBG); err != nil || has {
		t.Fatalf("HasData(PVMBG) = %v, %v", has, err)
	}
}

func testSources(t *testing.T, b *storage.Backend) {
	key := domain.SourceKeyBMKGSeismic
	st, err := b.Sources.Get(ctx(t), key)
	if err != nil || st.Status != domain.StatusUnavailable || !st.Watermark.IsZero() || !st.LastSuccessAt.IsZero() {
		t.Fatalf("unknown stream = %+v, %v", st, err)
	}

	// A first failure leaves no watermark and no success.
	if err := b.Sources.RecordFailure(ctx(t), key, "down"); err != nil {
		t.Fatal(err)
	}
	st, _ = b.Sources.Get(ctx(t), key)
	if st.Status != domain.StatusStale || st.ErrorMessage != "down" || !st.Watermark.IsZero() ||
		!st.LastSuccessAt.IsZero() || st.LastAttemptAt.IsZero() {
		t.Fatalf("after first failure: %+v", st)
	}

	if err := b.Sources.RecordSuccess(ctx(t), key, base); err != nil {
		t.Fatal(err)
	}
	st, _ = b.Sources.Get(ctx(t), key)
	if st.Status != domain.StatusFresh || st.ErrorMessage != "" || !st.Watermark.Equal(base) || st.LastSuccessAt.IsZero() {
		t.Fatalf("after success: %+v", st)
	}
	success := st.LastSuccessAt

	if err := b.Sources.RecordFailure(ctx(t), key, "timeout"); err != nil {
		t.Fatal(err)
	}
	st, _ = b.Sources.Get(ctx(t), key)
	if st.Status != domain.StatusStale || st.ErrorMessage != "timeout" || !st.Watermark.Equal(base) ||
		!st.LastSuccessAt.Equal(success) {
		t.Fatalf("failure must keep watermark and last success: %+v", st)
	}

	// Streams are independent.
	other, _ := b.Sources.Get(ctx(t), domain.SourceKeyPVMBGVolcanic)
	if other.Status != domain.StatusUnavailable {
		t.Fatalf("other stream = %+v", other)
	}
}

// enqueue stores n new hazards, so n events, and returns them oldest first.
func enqueue(t *testing.T, b *storage.Backend, n int) []storage.OutboxEvent {
	rec := &eventRecorder{}
	for i := 0; i < n; i++ {
		ref := "o-" + uuid.New().String()
		apply(t, b, rec, hazard(domain.SourceBMKG, ref, base.Add(time.Duration(i)*time.Second)))
		// Distinct creation times keep the expected order unambiguous.
		time.Sleep(3 * time.Millisecond)
	}
	return rec.events
}

func claim(t *testing.T, b *storage.Backend, limit int, lease time.Duration) []*storage.ClaimedEvent {
	t.Helper()
	got, err := b.Outbox.ClaimDue(ctx(t), limit, lease)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	return got
}

func eventIDs(evs []*storage.ClaimedEvent) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.EventID
	}
	return out
}

func testOutboxClaimAck(t *testing.T, b *storage.Backend) {
	events := enqueue(t, b, 3)
	got := claim(t, b, 10, time.Minute)
	if len(got) != 3 {
		t.Fatalf("claimed %d of 3", len(got))
	}
	for i, ev := range got {
		want := events[i]
		if ev.EventID != want.EventID || ev.HazardID != want.HazardID || ev.Source != want.Source ||
			string(ev.Payload) != string(want.Payload) || ev.TraceContext["traceparent"] != want.TraceContext["traceparent"] ||
			ev.RetryCount != 0 || ev.CreatedAt.IsZero() {
			t.Fatalf("claim %d = %+v, want %+v (oldest first)", i, ev, want)
		}
	}
	for _, ev := range got {
		if err := b.Outbox.Ack(ctx(t), ev.EventID); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Outbox.Release(ctx(t), eventIDs(got)); err != nil {
		t.Fatal(err)
	}
	if again := claim(t, b, 10, 0); len(again) != 0 {
		t.Fatalf("acked events claimed again: %v", eventIDs(again))
	}
	if n, err := b.Outbox.Prune(ctx(t), time.Hour, 100); err != nil || n != 0 {
		t.Fatalf("Prune of fresh events = %d, %v", n, err)
	}
}

func testOutboxLease(t *testing.T, b *storage.Backend) {
	enqueue(t, b, 2)
	if got := claim(t, b, 1, time.Minute); len(got) != 1 {
		t.Fatalf("limit 1 claimed %d", len(got))
	}
	if got := claim(t, b, 10, time.Minute); len(got) != 1 {
		t.Fatalf("leased event claimed again: %d claimed", len(got))
	}
	if got := claim(t, b, 10, time.Minute); len(got) != 0 {
		t.Fatalf("leased events claimed again: %v", eventIDs(got))
	}

	// An expired lease makes the event due again.
	b2 := enqueue(t, b, 1)
	first := claim(t, b, 10, 300*time.Millisecond)
	if len(first) != 1 || first[0].EventID != b2[0].EventID {
		t.Fatalf("claim = %v", eventIDs(first))
	}
	time.Sleep(time.Second)
	if again := claim(t, b, 10, time.Minute); len(again) != 1 || again[0].EventID != b2[0].EventID {
		t.Fatalf("expired lease not reclaimed: %v", eventIDs(again))
	}
}

func testOutboxRetryRelease(t *testing.T, b *storage.Backend) {
	enqueue(t, b, 2)
	got := claim(t, b, 10, time.Minute)
	if len(got) != 2 {
		t.Fatalf("claimed %d", len(got))
	}
	if err := b.Outbox.RetryLater(ctx(t), got[0].EventID, "nack", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := b.Outbox.Release(ctx(t), []string{got[1].EventID}); err != nil {
		t.Fatal(err)
	}
	again := claim(t, b, 10, time.Minute)
	if len(again) != 1 || again[0].EventID != got[1].EventID {
		t.Fatalf("after retry+release claimed %v, want only %s", eventIDs(again), got[1].EventID)
	}

	if err := b.Outbox.RetryLater(ctx(t), again[0].EventID, "nack", 0); err != nil {
		t.Fatal(err)
	}
	if err := b.Outbox.RetryLater(ctx(t), again[0].EventID, "nack", 0); err != nil {
		t.Fatal(err)
	}
	retried := claim(t, b, 10, time.Minute)
	if len(retried) != 1 || retried[0].RetryCount != 2 {
		t.Fatalf("retry count not kept: %+v", retried)
	}
}

func testOutboxDeadLetter(t *testing.T, b *storage.Backend) {
	enqueue(t, b, 1)
	got := claim(t, b, 10, time.Minute)
	if len(got) != 1 {
		t.Fatalf("claimed %d", len(got))
	}
	if err := b.Outbox.DeadLetter(ctx(t), got[0], "max retries"); err != nil {
		t.Fatal(err)
	}
	if err := b.Outbox.Release(ctx(t), eventIDs(got)); err != nil {
		t.Fatal(err)
	}
	if again := claim(t, b, 10, 0); len(again) != 0 {
		t.Fatalf("dead letter claimed again: %v", eventIDs(again))
	}
}

func testOutboxConcurrent(t *testing.T, b *storage.Backend) {
	if b.OutboxSingleConsumer {
		t.Skip("backend allows a single outbox consumer")
	}
	enqueue(t, b, 20)
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := b.Outbox.ClaimDue(context.Background(), 20, time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			for _, ev := range got {
				seen[ev.EventID]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	for id, n := range seen {
		if n > 1 {
			t.Fatalf("event %s claimed %d times by concurrent claimers", id, n)
		}
	}
	if len(seen) != 20 {
		t.Fatalf("claimed %d distinct events of 20", len(seen))
	}
}

func isInvalidToken(err error) bool { return errors.Is(err, storage.ErrInvalidPageToken) }
