//go:build integration

package cassandra

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

func TestCassandraProductionWritesFenceExpiredAndReleasedTerms(t *testing.T) {
	db, err := openTest(t, "agg_test_fence_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()
	l := leases{db}
	name := "test-coastal-poller"
	if ok, err := l.Acquire(ctx, name, "old", 5*time.Second); err != nil || !ok {
		t.Fatal(ok, err)
	}
	a, err := l.Fence(ctx, name, "old")
	if err != nil {
		t.Fatal(err)
	}
	old := leader.WithAuthority(ctx, a, l)
	h := &domain.HazardRecord{Source: domain.SourceCoastal, SourceRefID: "known", HazardType: domain.HazardTypeCoastal, Severity: domain.SeverityWaspada, OccurredAt: time.Now(), Attributes: map[string]any{"critical": true}}
	if _, _, err := (hazards{db}).ApplyRevision(old, h, revEvent); err != nil {
		t.Fatal(err)
	}
	message := storage.IngestMessage{ID: "msg-00" + strings.Repeat("a", 62), Source: domain.SourceCoastal, SourceRefID: "known", Stream: "coastal", Format: "m2-raw", State: storage.IngestParked, Raw: []byte(`{}`), ReceivedAt: time.Now()}
	if err := (ingestInbox{db}).Put(old, message); err != nil {
		t.Fatal(err)
	}
	if err := (sources{db}).RecordSuccess(old, domain.SourceKeyCoastal, time.Now()); err != nil {
		t.Fatal(err)
	}
	stream := streams{db}
	if _, err := stream.Append(old, "coastal", storage.StreamEntry{Sequence: 1, Kind: "observations", Cursor: "1", Payload: []byte(`{}`), ReceivedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Commit(old, "coastal", 0, storage.StreamCheckpoint{Sequence: 1, Cursor: "1"}); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	resumed := false
	defer func() {
		if !resumed {
			close(resume)
		}
	}()
	paused := *h
	paused.SourceRefID = "paused-before-cas"
	pausedResult := make(chan error, 1)
	go func() {
		_, _, err := (hazards{db}).ApplyRevision(old, &paused, func(saved *domain.HazardRecord) (storage.OutboxEvent, error) {
			close(entered)
			<-resume
			return revEvent(saved)
		})
		pausedResult <- err
	}()
	<-entered
	// Wait for storage's expiry while deliberately retaining an uncancelled Go
	// context. No local elector/timer is involved in the rejection.
	for {
		s, err := l.read(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if !s.live() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if old.Err() != nil {
		t.Fatal("fixture context cancelled")
	}
	lost := func(label string, err error) {
		t.Helper()
		if !errors.Is(err, leader.ErrLostAuthority) {
			t.Fatalf("%s accepted expired authority: %v", label, err)
		}
	}
	fresh := *h
	fresh.SourceRefID = "unseen"
	_, _, err = (hazards{db}).ApplyRevision(old, &fresh, revEvent)
	lost("new canonical record", err)
	h.Severity = domain.SeverityAwas
	_, _, err = (hazards{db}).ApplyRevision(old, h, revEvent)
	lost("canonical revision", err)
	message.ID = "msg-00" + strings.Repeat("b", 62)
	lost("new intake", (ingestInbox{db}).Put(old, message))
	message.SourceRefID = ""
	message.ID = "msg-00" + strings.Repeat("c", 62)
	lost("raw quarantine", (ingestInbox{db}).Put(old, message))
	_, err = stream.Append(old, "coastal", storage.StreamEntry{Sequence: 1001, Kind: "observations", Cursor: "1001", Payload: []byte(`{}`), ReceivedAt: time.Now()})
	lost("unseen journal block", err)
	lost("checkpoint", stream.Commit(old, "coastal", 1, storage.StreamCheckpoint{Sequence: 2, Cursor: "2"}))
	lost("source state", (sources{db}).RecordFailure(old, domain.SourceKeyCoastal, "stale writer"))
	lost("fallback quarantine", stream.Park(old, storage.ParkedInput{Source: domain.SourceCoastal, MessageID: "unseen", Payload: []byte(`{}`), Reason: "test", ReceivedAt: time.Now()}))
	if ok, err := l.Acquire(ctx, name, "new", 30*time.Second); err != nil || !ok {
		t.Fatal(ok, err)
	}
	b, err := l.Fence(ctx, name, "new")
	if err != nil || b.Epoch <= a.Epoch {
		t.Fatalf("invalid new term %+v %v", b, err)
	}
	close(resume)
	resumed = true
	lost("paused between read/build and canonical CAS", <-pausedResult)
	if record, err := (hazards{db}).Get(ctx, domain.HazardID(paused.Source, paused.SourceRefID)); err != nil || record != nil {
		t.Fatal("paused old writer committed after takeover", record, err)
	}
	_, _, err = (hazards{db}).ApplyRevision(old, &fresh, revEvent)
	lost("superseded context", err)
	current := leader.WithAuthority(ctx, b, l)
	if _, _, err := (hazards{db}).ApplyRevision(current, &fresh, revEvent); err != nil {
		t.Fatal("current term failed", err)
	}
	state, err := l.read(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.Release(ctx, name, "new"); err != nil {
		t.Fatal(err)
	}
	fresh.SourceRefID = "after-release"
	_, _, err = (hazards{db}).ApplyRevision(current, &fresh, revEvent)
	lost("released context", err)
	if err = db.installAuthority(ctx, canonicalGuard(domain.HazardID(fresh.Source, fresh.SourceRefID)), b, *state.until); !errors.Is(err, leader.ErrLostAuthority) {
		t.Fatal("delayed activation restored released authority", err)
	}
	stored, err := (hazards{db}).Get(ctx, domain.HazardID(h.Source, h.SourceRefID))
	if err != nil || stored.Severity != domain.SeverityWaspada || stored.Revision != 1 {
		t.Fatalf("stale canonical data persisted %+v %v", stored, err)
	}
	if missing, err := (hazards{db}).Get(ctx, domain.HazardID(fresh.Source, fresh.SourceRefID)); err != nil || missing != nil {
		t.Fatal("released writer persisted a new hazard", missing, err)
	}
	if p, err := stream.Checkpoint(ctx, "coastal"); err != nil || p.Sequence != 1 {
		t.Fatal("stale checkpoint committed", p, err)
	}
	critical, err := db.Backend().Critical.Pending(ctx, storage.CriticalCursor{}, 100)
	if err != nil || len(critical.Events) != 2 {
		t.Fatal("fenced writer created/lost critical history", critical, err)
	}
}
