//go:build integration

package m2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/cassandra"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/postgres"
	"github.com/google/uuid"
)

type observedSnapshots struct {
	storage.StreamSnapshots
	storage.StreamingSnapshots
	once  sync.Once
	saved chan struct{}
}

func (s *observedSnapshots) SaveSnapshotFrom(ctx context.Context, stream string, seq int64, r io.ReadSeeker) error {
	err := s.StreamingSnapshots.SaveSnapshotFrom(ctx, stream, seq, r)
	if err == nil {
		s.once.Do(func() { close(s.saved) })
	}
	return err
}

type recoveryStreams struct {
	storage.StreamStore
	saved     <-chan struct{}
	failAt    int64
	firstRead int64
	read      bool
	cancelAt  int64
	cancel    context.CancelFunc
	parked    int
}

var errInjectedCheckpoint = errors.New("injected checkpoint failure after canonical save")

func (s *recoveryStreams) Commit(ctx context.Context, stream string, before int64, p storage.StreamCheckpoint) error {
	if p.Sequence == s.failAt {
		select {
		case <-s.saved:
			return errInjectedCheckpoint
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := s.StreamStore.Commit(ctx, stream, before, p)
	if err == nil && p.Sequence == s.cancelAt {
		s.cancel()
	}
	return err
}
func (s *recoveryStreams) Read(ctx context.Context, stream string, after int64, limit int) ([]storage.StreamEntry, error) {
	if !s.read {
		s.firstRead = after
		s.read = true
	}
	return s.StreamStore.Read(ctx, stream, after, limit)
}
func (s *recoveryStreams) Park(ctx context.Context, input storage.ParkedInput) error {
	s.parked++
	return s.StreamStore.Park(ctx, input)
}

func TestSatelliteSnapshotRecoveryPostgres(t *testing.T) {
	dsn := os.Getenv("AGG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("AGG_TEST_POSTGRES_DSN required; disposable data only")
	}
	db, err := postgres.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Pool.Close()
	testSatelliteSnapshotRecovery(t, db.Backend())
}
func TestSatelliteSnapshotRecoveryCassandra(t *testing.T) {
	hosts := os.Getenv("AGG_TEST_CASSANDRA_HOSTS")
	if hosts == "" {
		t.Fatal("AGG_TEST_CASSANDRA_HOSTS required; disposable data only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := cassandra.Open(ctx, cassandra.Config{Hosts: strings.Split(hosts, ","), Keyspace: "agg_satellite_" + strings.ReplaceAll(uuid.NewString(), "-", ""), Consistency: "LOCAL_QUORUM", BucketPeriod: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	testSatelliteSnapshotRecovery(t, db.Backend())
}

func testSatelliteSnapshotRecovery(t *testing.T, b *storage.Backend) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// A unique latitude also isolates canonical IDs between repeated test runs.
	lat := fmt.Sprintf("%.8f", float64(time.Now().UnixNano()%1000000000)/1000000000)
	inputs := [][]byte{detection("N20", "600", lat, "0", "n", 10), detection("N21", "601", lat, "0.001", "h", 20), detection("N20", "602", lat, "0.002", "n", 3), detection("N21", "603", lat, "0.003", "h", 7), detection("N20", "604", lat, "0.004", "n", 11)}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		after, _ := strconv.Atoi(q.URL.Query().Get("from"))
		for i, raw := range inputs {
			if i+1 > after {
				fmt.Fprintf(w, "id: %d\nevent: detection\ndata: %s\n\n", i+1, raw)
			}
		}
		w.(http.Flusher).Flush()
		<-q.Context().Done()
	}))
	defer upstream.Close()
	baseStreams := b.Streams
	observed := &observedSnapshots{StreamSnapshots: b.Snapshots, StreamingSnapshots: b.Snapshots.(storage.StreamingSnapshots), saved: make(chan struct{})}
	b.Snapshots = observed
	failed := &recoveryStreams{StreamStore: baseStreams, saved: observed.saved, failAt: 4}
	b.Streams = failed
	root := t.TempDir()
	cfg := Config{SatelliteURL: upstream.URL, SatelliteToken: "test", SatelliteStream: "recovery-" + uuid.NewString(), StateDir: root, Lateness: 24 * time.Hour, Retention: 48 * time.Hour, SnapshotEvery: 2, MaxDetections: 100, MaxBodyBytes: 1 << 20}
	if err := NewRunner(cfg, b).satellite(ctx); !errors.Is(err, errInjectedCheckpoint) {
		t.Fatal("expected checkpoint failure", err)
	}
	if failed.parked != 0 {
		t.Fatal("canonical/operational error was parked")
	}
	p, err := baseStreams.Checkpoint(ctx, cfg.SatelliteStream)
	if err != nil || p.Sequence != 3 {
		t.Fatal("checkpoint advanced on failure", p, err)
	}
	// This must open an actual replicated snapshot, not silently replay zero.
	d, seq, err := NewRunner(cfg, b).restoreSatellite(ctx, cfg.SatelliteStream, 3)
	if err != nil || seq != 2 {
		t.Fatal("snapshot not available", seq, err)
	}
	_ = d.Close()
	restartCtx, stop := context.WithCancel(ctx)
	defer stop()
	restarted := &recoveryStreams{StreamStore: baseStreams, cancelAt: 5, cancel: stop}
	b.Streams = restarted
	_ = NewRunner(cfg, b).satellite(restartCtx)
	if !restarted.read || restarted.firstRead != 2 {
		t.Fatal("recovery replay did not start at snapshot tail", restarted.firstRead)
	}
	p, err = baseStreams.Checkpoint(ctx, cfg.SatelliteStream)
	if err != nil || p.Sequence != 5 {
		t.Fatal("recovery incomplete", p, err)
	}
	ref := NewFireEngineRetention(cfg.Lateness, cfg.Retention, cfg.MaxDetections)
	for i, raw := range inputs {
		if _, err := ref.Apply("detection", raw, int64(i+1), testNow); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range ref.Snapshot() {
		got, err := b.Hazards.Get(ctx, domain.HazardID(want.Source, want.SourceRefID))
		if err != nil || got == nil {
			t.Fatal("canonical fire missing", err)
		}
		if fmt.Sprint(got.Attributes["detection_count"]) != "5" || fmt.Sprint(got.Attributes["stream_sequence"]) != "5" || got.Revision != 4 {
			t.Fatal("lost or duplicated revision after recovery", got)
		}
	}
	files, _ := os.ReadDir(root)
	if len(files) != 0 {
		t.Fatal("term working state or upload leaked", files)
	}
}
