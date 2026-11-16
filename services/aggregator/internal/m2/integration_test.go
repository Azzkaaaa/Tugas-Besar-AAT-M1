//go:build integration

package m2

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/postgres"
)

type failCommit struct {
	storage.StreamStore
	failed bool
}

func (s *failCommit) Commit(ctx context.Context, stream string, before int64, p storage.StreamCheckpoint) error {
	if p.Sequence == 2 && !s.failed {
		s.failed = true
		return errors.New("injected crash after hazard/outbox write")
	}
	return s.StreamStore.Commit(ctx, stream, before, p)
}

func TestPostgresCrashBeforeCheckpoint(t *testing.T) {
	dsn := os.Getenv("AGG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("AGG_TEST_POSTGRES_DSN required; use a disposable test database")
	}
	db, err := postgres.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Pool.Close()
	if _, err = db.Pool.Exec(context.Background(), `TRUNCATE m2_stream_journal,m2_stream_checkpoints,m2_parked_inputs,hazard_events,outbox_events,outbox_dead_letters`); err != nil {
		t.Fatal(err)
	}
	inputs := [][]byte{detection("N20", "600", "0", "0", "n", 10), detection("N21", "700", "0", "0.001", "h", 20), detection("N20", "800", "0", "0.002", "n", 3)}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		start, _ := strconv.Atoi(q.URL.Query().Get("from"))
		for i := start; i < len(inputs); i++ {
			fmt.Fprintf(w, "id: %d\nevent: detection\ndata: %s\n\n", i, inputs[i])
		}
		w.(http.Flusher).Flush()
		<-q.Context().Done()
	}))
	defer upstream.Close()
	cfg := Config{SatelliteURL: upstream.URL, SatelliteToken: "test", SatelliteStream: "recovery-test", Lateness: 24 * time.Hour, MaxDetections: 100, MaxBodyBytes: 1 << 20}
	backend := db.Backend()
	failure := &failCommit{StreamStore: backend.Streams}
	backend.Streams = failure
	r := NewRunner(cfg, backend)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = r.satellite(ctx); err == nil || !failure.failed {
		t.Fatalf("expected injected crash: %v", err)
	}
	p, _ := failure.Checkpoint(ctx, cfg.SatelliteStream)
	if p.Sequence != 1 {
		t.Fatalf("advanced checkpoint despite crash: %+v", p)
	}
	backend = db.Backend()
	r = NewRunner(cfg, backend)
	recoveryCtx, stop := context.WithTimeout(ctx, 800*time.Millisecond)
	defer stop()
	_ = r.satellite(recoveryCtx)
	p, err = backend.Streams.Checkpoint(ctx, cfg.SatelliteStream)
	if err != nil || p.Sequence != 3 {
		t.Fatalf("recovery cursor: %+v %v", p, err)
	}
	page, err := backend.Hazards.List(ctx, storage.HazardQuery{}, storage.PageRequest{Size: 100})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("%+v %v", page, err)
	}
	if count := page.Items[0].Attributes["detection_count"]; count != float64(3) {
		t.Fatalf("count %v", count)
	}
	var n int
	if err = db.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("outbox duplicate/loss: %d %v", n, err)
	}
	if err = backend.Streams.Commit(ctx, cfg.SatelliteStream, 1, storage.StreamCheckpoint{Sequence: 2}); !errors.Is(err, storage.ErrCheckpointConflict) {
		t.Fatal("stale checkpoint writer accepted")
	}
}

// This opt-in test exercises the unmodified assistant image, not a replacement mock.
func TestOfficialCoastalImage(t *testing.T) {
	base := os.Getenv("PANTAI_TEST_URL")
	if base == "" {
		t.Skip("PANTAI_TEST_URL not set")
	}
	db, err := postgres.Connect(context.Background(), os.Getenv("AGG_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	r := NewRunner(Config{CoastalURL: base, CoastalToken: "m2-local-test-only", CoastalPoll: 50 * time.Millisecond, Timeout: time.Second, StaleAfter: 2 * time.Minute, OpenFor: time.Second, FailureThreshold: 3, MaxBodyBytes: 2 << 20}, db.Backend())
	_ = r.coastal(ctx)
	check, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	page, err := db.Backend().Hazards.List(check, storage.HazardQuery{Source: "SENSOR_PANTAI"}, storage.PageRequest{Size: 1000})
	if err != nil || len(page.Items) == 0 {
		t.Fatalf("no official observations stored: %v", err)
	}
	critical := 0
	for _, h := range page.Items {
		if h.Attributes["critical"] == true {
			critical++
			if h.Severity != "AWAS" {
				t.Fatal("critical notification would be filtered")
			}
		}
		if h.HazardType != "COASTAL" {
			t.Fatal("wrong hazard type")
		}
	}
	p, err := db.Backend().Streams.Checkpoint(check, "coastal-v1")
	if err != nil || p.Sequence == 0 {
		t.Fatalf("no checkpoint: %+v %v", p, err)
	}
	t.Logf("official sensor: %d records on first audit page, %d critical, checkpoint %d", len(page.Items), critical, p.Sequence)
}
