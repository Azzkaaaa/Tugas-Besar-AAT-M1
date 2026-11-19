package m2

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

// Byte methods deliberately fail: production must never use the heap-sized
// fallback even when a test backend advertises both interfaces.
type snapshotTestStore struct {
	storage.StreamSnapshots
	mu        sync.Mutex
	sequence  int64
	state     []byte
	started   chan struct{}
	release   chan struct{}
	openError error
}

func (s *snapshotTestStore) SaveSnapshotFrom(ctx context.Context, _ string, seq int64, r io.ReadSeeker) error {
	if s.started != nil {
		close(s.started)
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequence, s.state = seq, b
	return nil
}
func (s *snapshotTestStore) OpenLatestSnapshot(_ context.Context, _ string, atMost int64, _ string) (int64, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.openError != nil {
		return 0, nil, s.openError
	}
	if s.sequence == 0 || s.sequence > atMost {
		return 0, nil, nil
	}
	return s.sequence, io.NopCloser(bytes.NewReader(bytes.Clone(s.state))), nil
}

func TestSatelliteSnapshotUploadIsBoundedAndRestoresDiskState(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	d := testDiskProcessor(t, 100)
	if err := d.ApplyTo(ctx, "detection", detection("N20", "600", "0", "0", "n", 10), 1, testNow, nil); err != nil {
		t.Fatal(err)
	}
	backend := &snapshotTestStore{started: make(chan struct{}), release: make(chan struct{})}
	job := newSatelliteSnapshotter(ctx, backend, root)
	defer job.Close()
	if scheduled, err := job.Save(ctx, "test", 1, d); err != nil || !scheduled {
		t.Fatal(scheduled, err)
	}
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("upload did not start")
	}
	// Processing continues while remote writes are blocked; another snapshot
	// does not accumulate a queue, goroutine, file or a whole extra state.
	if err := d.ApplyTo(ctx, "heartbeat", []byte("{}"), 2, testNow, nil); err != nil {
		t.Fatal(err)
	}
	if scheduled, err := job.Save(ctx, "test", 2, d); err != nil || scheduled {
		t.Fatal(scheduled, err)
	}
	files, _ := os.ReadDir(root)
	if len(files) != 1 {
		t.Fatal("queued extra snapshots", files)
	}
	close(backend.release)
	select {
	case <-job.done:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not finish")
	}
	r := NewRunner(Config{StateDir: root, Lateness: 24 * time.Hour, Retention: 48 * time.Hour, MaxDetections: 100, SnapshotEvery: 1}, &storage.Backend{Snapshots: backend})
	restored, seq, err := r.restoreSatellite(ctx, "test", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if seq != 1 || restored.Active() != 1 || restored.lastSequence != 1 {
		t.Fatal("snapshot state differs", seq, restored.Active())
	}
	if err := restored.ApplyTo(ctx, "heartbeat", []byte("{}"), 2, testNow, nil); err != nil {
		t.Fatal(err)
	}
	var got, want bytes.Buffer
	if err := restored.WriteState(ctx, &got, 2); err != nil {
		t.Fatal(err)
	}
	if err := d.WriteState(ctx, &want, 2); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatal("restored processing changed state")
	}
}

func TestSatelliteSnapshotCancellationJoinsAndRemovesSpool(t *testing.T) {
	root := t.TempDir()
	backend := &snapshotTestStore{started: make(chan struct{}), release: make(chan struct{})}
	d := testDiskProcessor(t, 100)
	job := newSatelliteSnapshotter(context.Background(), backend, root)
	if _, err := job.Save(context.Background(), "test", 0, d); err != nil {
		t.Fatal(err)
	}
	<-backend.started
	job.Close()
	files, _ := os.ReadDir(root)
	if len(files) != 0 {
		t.Fatal("canceled upload leaked", files)
	}
}

func TestSatelliteRestoreRejectsBadCacheAndFailsOnLocalStateError(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend := &snapshotTestStore{sequence: 10, state: []byte("corrupt")}
	r := NewRunner(Config{StateDir: root, Lateness: 24 * time.Hour, Retention: 48 * time.Hour, MaxDetections: 100, SnapshotEvery: 1}, &storage.Backend{Snapshots: backend})
	d, seq, err := r.restoreSatellite(ctx, "test", 10)
	if err != nil || seq != 0 || d.Active() != 0 {
		t.Fatal("bad cache did not replay", seq, err)
	}
	_ = d.Close()
	backend.openError = &os.PathError{Op: "create", Path: root, Err: os.ErrPermission}
	if _, _, err := r.restoreSatellite(ctx, "test", 10); !errors.Is(err, errWorkingState) {
		t.Fatal("local disk failure ignored", err)
	}
	files, _ := os.ReadDir(root)
	if len(files) != 0 {
		t.Fatal("failed restore leaked working directories", files)
	}
}

func TestSatelliteExternalQuarantineMarkerIsInvalidInput(t *testing.T) {
	d := testDiskProcessor(t, 100)
	err := ReadSSE(bytes.NewBufferString("id: 1\nevent: quarantined\ndata: {\"untrusted\":true}\n\n"), 1024, func(f sseFrame) error {
		if f.archived {
			t.Fatal("external event marked archived")
		}
		return d.ApplyTo(context.Background(), satelliteJournalKind(f), f.Data, 1, testNow, nil)
	})
	if !errors.Is(err, errFireInput) {
		t.Fatal("external event silently skipped", err)
	}
	if kind := satelliteJournalKind(sseFrame{Kind: "detection", archived: true}); kind != "quarantined" {
		t.Fatal(kind)
	}
}
