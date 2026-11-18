//go:build integration

package cassandra

import (
	"context"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

func TestStreamUpgradeMergesLegacyAndGuardedBlocks(t *testing.T) {
	db, err := openTest(t, "agg_test_stream_upgrade_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()
	s := streams{db}
	name := "upgrade"
	for seq := int64(998); seq <= 1000; seq++ {
		if _, err := s.legacyAppend(ctx, name, storage.StreamEntry{Sequence: seq, Kind: "old", Cursor: "old", Payload: []byte("original"), ReceivedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.legacyCommit(ctx, name, 0, storage.StreamCheckpoint{Sequence: 1000, Cursor: "old"}); err != nil {
		t.Fatal(err)
	}
	for seq := int64(1001); seq <= 1003; seq++ {
		if _, err := s.Append(ctx, name, storage.StreamEntry{Sequence: seq, Kind: "new", Cursor: "new", Payload: []byte("new"), ReceivedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	// Duplicate retries return the original legacy input without replacing bytes.
	duplicate, err := s.Append(ctx, name, storage.StreamEntry{Sequence: 999, Payload: []byte("replacement")})
	if err != nil || string(duplicate.Payload) != "original" {
		t.Fatal("legacy duplicate replaced", duplicate, err)
	}
	rows, err := s.Read(ctx, name, 997, 10)
	if err != nil || len(rows) != 6 {
		t.Fatalf("cross-layout read: %d %v", len(rows), err)
	}
	for i, row := range rows {
		if row.Sequence != int64(998+i) {
			t.Fatal("cross-layout sequence gap", rows)
		}
	}
	if err := s.Commit(ctx, name, 1000, storage.StreamCheckpoint{Sequence: 1003, Cursor: "new"}); err != nil {
		t.Fatal("legacy checkpoint adoption", err)
	}
	if p, err := s.Checkpoint(ctx, name); err != nil || p.Sequence != 1003 || p.Cursor != "new" {
		t.Fatal("adopted checkpoint lost", p, err)
	}
	if err := s.Commit(ctx, name, 1000, storage.StreamCheckpoint{Sequence: 1004}); err == nil {
		t.Fatal("stale checkpoint overwrote current state")
	}
}
