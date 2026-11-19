package m2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func satelliteJournalKind(f sseFrame) string {
	if f.archived {
		return "quarantined"
	}
	if f.Kind == "quarantined" {
		return "external:quarantined"
	}
	return f.Kind
}

// A bad/incompatible recovery cache falls back to the authoritative journal.
// Failure to create or use local working state ends this term instead.
func (r *Runner) restoreSatellite(ctx context.Context, stream string, committed int64) (*diskFireProcessor, int64, error) {
	fresh, err := newDiskFireProcessor(ctx, r.Config.StateDir, r.Config.Lateness, r.Config.Retention, r.Config.MaxDetections)
	if err != nil {
		return nil, 0, workingError(err)
	}
	backend, ok := r.Store.Snapshots.(storage.StreamingSnapshots)
	if !ok || r.Config.SnapshotEvery == 0 || committed == 0 {
		return fresh, 0, nil
	}
	seq, input, err := backend.OpenLatestSnapshot(ctx, stream, committed, fresh.graph.directory)
	if err == nil && input != nil {
		var restored *diskFireProcessor
		restored, err = restoreDiskFireProcessor(ctx, r.Config.StateDir, r.Config.Lateness, r.Config.Retention, r.Config.MaxDetections, seq, input)
		closeErr := input.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			_ = fresh.Close()
			return restored, seq, nil
		}
		if restored != nil {
			_ = restored.Close()
		}
	}
	var pathError *os.PathError
	if errors.Is(err, errWorkingState) || errors.As(err, &pathError) || ctx.Err() != nil {
		_ = fresh.Close()
		return nil, 0, workingError(errors.Join(err, ctx.Err()))
	}
	if err != nil {
		slog.WarnContext(ctx, "satellite snapshot rejected; replaying journal", "error", err, "correlation_id", "satellite-recovery")
	}
	return fresh, 0, nil
}

type satelliteSnapshotter struct {
	backend storage.StreamingSnapshots
	root    string
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
}

func newSatelliteSnapshotter(ctx context.Context, backend storage.StreamSnapshots, root string) *satelliteSnapshotter {
	jobCtx, cancel := context.WithCancel(ctx)
	streaming, _ := backend.(storage.StreamingSnapshots)
	return &satelliteSnapshotter{backend: streaming, root: root, ctx: jobCtx, cancel: cancel}
}

// Save serializes a consistent committed state to local disk, then lets exactly
// one uploader perform remote chunk writes without blocking further ingestion.
// A busy uploader does not build a queue or retain another state on the heap.
func (s *satelliteSnapshotter) Save(ctx context.Context, stream string, sequence int64, d *diskFireProcessor) (bool, error) {
	if s.backend == nil {
		return false, nil
	}
	if s.done != nil {
		select {
		case <-s.done:
			s.done = nil
		default:
			return false, nil
		}
	}
	f, err := os.CreateTemp(s.root, "snapshot-upload-*")
	if err != nil {
		return false, workingError(err)
	}
	discard := func() { _ = f.Close(); _ = os.Remove(f.Name()) }
	if err := d.WriteState(ctx, f, sequence); err != nil {
		discard()
		return false, workingError(err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		discard()
		return false, workingError(err)
	}
	done := make(chan struct{})
	s.done = done
	go func() {
		defer close(done)
		defer discard()
		uploadCtx, cancel := context.WithTimeout(s.ctx, 2*time.Minute)
		defer cancel()
		err := s.backend.SaveSnapshotFrom(uploadCtx, stream, sequence, f)
		if err != nil && s.ctx.Err() == nil {
			slog.WarnContext(s.ctx, "satellite snapshot upload failed", "sequence", sequence, "error", err, "correlation_id", fmt.Sprintf("satellite-%d", sequence))
		}
	}()
	return true, nil
}

func (s *satelliteSnapshotter) Close() {
	s.cancel()
	if s.done != nil {
		<-s.done
		s.done = nil
	}
}
