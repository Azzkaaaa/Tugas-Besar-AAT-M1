//go:build integration

package cassandra

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/storetest"
	"github.com/google/uuid"
)

func TestStreamingSnapshots(t *testing.T) {
	db, err := openTest(t, "agg_test_streaming_snapshots", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer dropKeyspace(db)
	storetest.StreamingSnapshotContract(t, db.Backend())
	ctx := context.Background()
	s := snapshots{db}
	stream := "corrupt-" + uuid.NewString()
	dir := t.TempDir()
	for _, seq := range []int64{10, 20} {
		if err := s.SaveSnapshotFrom(ctx, stream, seq, bytes.NewReader([]byte("verified"))); err != nil {
			t.Fatal(err)
		}
	}
	var digest string
	if err := db.session.Query(`SELECT sha256 FROM m2_stream_snapshot_manifests WHERE stream=? AND sequence=?`, stream, int64(20)).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if err := db.session.Query(`UPDATE m2_stream_snapshot_parts SET data=? WHERE stream=? AND sequence=? AND sha256=? AND chunk=?`, []byte("corrupt"), stream, int64(20), digest, 0).Exec(); err != nil {
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
	if err := db.session.Query(`DELETE FROM m2_stream_snapshot_parts WHERE stream=? AND sequence=?`, stream, int64(30)).Exec(); err != nil {
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
