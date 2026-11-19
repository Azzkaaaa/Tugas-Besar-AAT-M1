//go:build integration

package postgres

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/storetest"
	"github.com/google/uuid"
)

func TestStreamingSnapshots(t *testing.T) {
	dsn := os.Getenv("AGG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("AGG_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	db, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Pool.Close()
	storetest.StreamingSnapshotContract(t, db.Backend())
	s := snapshots{db}
	stream := "corrupt-" + uuid.NewString()
	dir := t.TempDir()
	for _, seq := range []int64{10, 20} {
		if err := s.SaveSnapshotFrom(ctx, stream, seq, bytes.NewReader([]byte("verified"))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Pool.Exec(ctx, `UPDATE m2_stream_snapshot_parts SET data=$1 WHERE stream=$2 AND sequence=20`, []byte("corrupt"), stream); err != nil {
		t.Fatal(err)
	}
	seq, r, err := s.OpenLatestSnapshot(ctx, stream, 100, dir)
	if err != nil || seq != 10 || r == nil {
		t.Fatal("corruption did not fall back", seq, err)
	}
	b, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || string(b) != "verified" {
		t.Fatal(string(b), err)
	}
	if err := s.SaveSnapshotFrom(ctx, stream, 30, bytes.NewReader([]byte("verified"))); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `DELETE FROM m2_stream_snapshot_parts WHERE stream=$1 AND sequence=30`, stream); err != nil {
		t.Fatal(err)
	}
	seq, r, err = s.OpenLatestSnapshot(ctx, stream, 100, dir)
	if err != nil || seq != 0 || r != nil {
		t.Fatal("missing chunks accepted", seq, err)
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 0 {
		t.Fatal("corrupt downloads leaked", files)
	}
}
