package m2

import (
	"context"
	"errors"
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type boundedReplayStreams struct {
	storage.StreamStore
	read, largest int
	cancel        context.CancelFunc
}

func (s *boundedReplayStreams) Checkpoint(context.Context, string) (storage.StreamCheckpoint, error) {
	return storage.StreamCheckpoint{Sequence: 100, Cursor: "100"}, nil
}
func (s *boundedReplayStreams) Read(_ context.Context, _ string, after int64, limit int) ([]storage.StreamEntry, error) {
	s.largest = max(s.largest, limit)
	if after == 100 {
		s.cancel()
		return nil, context.Canceled
	}
	rows := make([]storage.StreamEntry, 0, limit)
	for seq := after + 1; seq <= 100 && len(rows) < limit; seq++ {
		rows = append(rows, storage.StreamEntry{Sequence: seq, Kind: "heartbeat", Payload: []byte("{}"), ReceivedAt: testNow})
		s.read++
	}
	return rows, nil
}

func TestSatelliteRecoveryReadsBoundedBatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streams := &boundedReplayStreams{cancel: cancel}
	r := NewRunner(Config{StateDir: t.TempDir()}, &storage.Backend{Streams: streams})
	if err := r.satellite(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if streams.read != 100 || streams.largest != satelliteRecoveryBatch || streams.largest > 8 {
		t.Fatal("recovery intake is unbounded or incomplete", streams.read, streams.largest)
	}
}
