//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

func authorityDB(t *testing.T) *DB {
	t.Helper()
	dsn := os.Getenv("AGG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("AGG_TEST_POSTGRES_DSN must point to a disposable database")
	}
	db, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Pool.Close)
	return db
}
func authorityEvent(h *domain.HazardRecord) (storage.OutboxEvent, error) {
	return storage.OutboxEvent{EventID: h.HazardID + uuid.NewString(), Source: h.Source, Payload: []byte(`{}`)}, nil
}
func authorityHazard(ref string) *domain.HazardRecord {
	return &domain.HazardRecord{Source: domain.SourceBMKG, SourceRefID: ref, HazardType: domain.HazardTypeSeismic, Severity: domain.SeverityWaspada, OccurredAt: time.Now().UTC()}
}

func TestWriteAuthorityRejectsExpiredAndSupersededLiveContexts(t *testing.T) {
	db := authorityDB(t)
	ctx := context.Background()
	l := leases{db}
	prefix := uuid.NewString()
	name := "fence-" + prefix
	if ok, err := l.Acquire(ctx, name, "same-pod", 20*time.Second); err != nil || !ok {
		t.Fatal(ok, err)
	}
	a, err := l.Fence(ctx, name, "same-pod")
	if err != nil {
		t.Fatal(err)
	}
	old := leader.WithAuthority(ctx, a, l)
	h := authorityHazard(prefix + "-existing")
	if _, _, err := (hazards{db}).ApplyRevision(old, h, authorityEvent); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `UPDATE leader_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE name=$1`, name); err != nil {
		t.Fatal(err)
	}
	// No context cancellation or local deadline is delivered: storage must fence.
	if old.Err() != nil {
		t.Fatal("fixture context unexpectedly cancelled")
	}
	fresh := authorityHazard(prefix + "-unseen")
	if _, _, err := (hazards{db}).ApplyRevision(old, fresh, authorityEvent); !errors.Is(err, leader.ErrLostAuthority) {
		t.Fatalf("expired leader created a new hazard: %v", err)
	}
	h.Severity = domain.SeverityAwas
	if _, _, err := (hazards{db}).ApplyRevision(old, h, authorityEvent); !errors.Is(err, leader.ErrLostAuthority) {
		t.Fatalf("expired leader updated a hazard: %v", err)
	}
	stream := streams{db}
	if _, err := stream.Append(old, prefix, storage.StreamEntry{Sequence: 1, Kind: "detection", Cursor: "1", Payload: []byte(`{}`), ReceivedAt: time.Now()}); !errors.Is(err, leader.ErrLostAuthority) {
		t.Fatal("expired journal append", err)
	}
	if err := stream.Commit(old, prefix, 0, storage.StreamCheckpoint{Sequence: 1, Cursor: "1"}); !errors.Is(err, leader.ErrLostAuthority) {
		t.Fatal("expired checkpoint", err)
	}
	if err := stream.Park(old, storage.ParkedInput{Source: "BMKG", MessageID: prefix, Payload: []byte(`{}`), ReceivedAt: time.Now(), Reason: "test"}); !errors.Is(err, leader.ErrLostAuthority) {
		t.Fatal("expired quarantine", err)
	}
	if err := (sources{db}).RecordSuccess(old, domain.SourceKeyBMKGSeismic, time.Now()); !errors.Is(err, leader.ErrLostAuthority) {
		t.Fatal("expired source state", err)
	}
	// Even deliberate reuse of the pod/holder string cannot reuse the epoch.
	if ok, err := l.Acquire(ctx, name, "same-pod", 20*time.Second); err != nil || !ok {
		t.Fatal(ok, err)
	}
	b, err := l.Fence(ctx, name, "same-pod")
	if err != nil || b.Epoch <= a.Epoch {
		t.Fatalf("epoch reused: %+v %+v %v", a, b, err)
	}
	if _, _, err := (hazards{db}).ApplyRevision(old, fresh, authorityEvent); !errors.Is(err, leader.ErrLostAuthority) {
		t.Fatal("superseded context accepted", err)
	}
	current := leader.WithAuthority(ctx, b, l)
	if _, _, err := (hazards{db}).ApplyRevision(current, fresh, authorityEvent); err != nil {
		t.Fatal("current term rejected", err)
	}
	stored, err := (hazards{db}).Get(ctx, domain.HazardID(h.Source, h.SourceRefID))
	if err != nil || stored.Severity != domain.SeverityWaspada || stored.Revision != 1 {
		t.Fatalf("stale revision leaked: %+v %v", stored, err)
	}
}

func TestFrozenTransactionRollsBackBeforeTakeover(t *testing.T) {
	db := authorityDB(t)
	ctx := context.Background()
	l := leases{db}
	prefix := uuid.NewString()
	name := "frozen-" + prefix
	if ok, err := l.Acquire(ctx, name, "old", 200*time.Millisecond); err != nil || !ok {
		t.Fatal(ok, err)
	}
	a, err := l.Fence(ctx, name, "old")
	if err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	h := authorityHazard(prefix)
	go func() {
		_, _, err := (hazards{db}).ApplyRevision(leader.WithAuthority(ctx, a, l), h, func(saved *domain.HazardRecord) (storage.OutboxEvent, error) {
			close(entered)
			<-resume
			return authorityEvent(saved)
		})
		done <- err
	}()
	<-entered
	time.Sleep(250 * time.Millisecond)
	start := time.Now()
	takeover, stop := context.WithTimeout(ctx, 7*time.Second)
	defer stop()
	ok, err := l.Acquire(takeover, name, "new", 20*time.Second)
	close(resume)
	if err != nil || !ok {
		t.Fatal("takeover blocked by frozen transaction", ok, err)
	}
	if time.Since(start) > 6*time.Second {
		t.Fatal("frozen transaction exceeded server-side bound")
	}
	if err := <-done; err == nil {
		t.Fatal("frozen writer committed after takeover")
	}
	stored, err := (hazards{db}).Get(ctx, domain.HazardID(h.Source, h.SourceRefID))
	if err != nil || stored != nil {
		t.Fatalf("rolled back hazard visible: %+v %v", stored, err)
	}
}
