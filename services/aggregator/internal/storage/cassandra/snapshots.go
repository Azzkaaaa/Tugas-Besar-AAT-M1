package cassandra

import (
	"bytes"
	"context"
	"errors"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"io"
	"time"
)

type snapshots struct{ db *DB }

// Unindexed uploads left by a killed/frozen client expire too. These are only
// recovery caches: an idle stream may replay its authoritative journal after
// seven days. Retained metadata/chunks use the same bounded lifetime.
const snapshotCacheTTL = 7 * 24 * 60 * 60

func (s snapshots) SaveSnapshot(ctx context.Context, stream string, sequence int64, state []byte) error {
	return s.SaveSnapshotFrom(ctx, stream, sequence, bytes.NewReader(state))
}
func (s snapshots) LatestSnapshot(ctx context.Context, stream string, atMost int64) (int64, []byte, error) {
	seq, r, err := s.OpenLatestSnapshot(ctx, stream, atMost, "")
	if err != nil || r == nil {
		return seq, nil, err
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	return seq, b, err
}
func (s snapshots) SaveSnapshotFrom(ctx context.Context, stream string, sequence int64, source io.ReadSeeker) error {
	size, digest, err := storage.SnapshotDigest(ctx, source)
	if err != nil {
		return err
	}
	chunks, err := storage.UploadSnapshotChunks(ctx, source, size, digest, func(index int, data []byte) error {
		return s.db.session.Query(`INSERT INTO m2_stream_snapshot_parts (stream,sequence,sha256,chunk,data) VALUES (?,?,?,?,?) USING TTL ?`, stream, sequence, digest, index, data, snapshotCacheTTL).
			WithContext(ctx).Consistency(gocql.LocalQuorum).Exec()
	})
	if err != nil {
		return err
	}
	// The first complete publication wins. A competing codec uploads under its
	// own digest and cannot overwrite the selected chunks, even after a pause.
	if _, err := s.db.session.Query(`INSERT INTO m2_stream_snapshot_manifests (stream,sequence,chunks,bytes,sha256,created_at) VALUES (?,?,?,?,?,?) IF NOT EXISTS USING TTL ?`, stream, sequence, chunks, size, digest, time.Now().UTC(), snapshotCacheTTL).
		WithContext(ctx).Consistency(gocql.LocalQuorum).SerialConsistency(gocql.LocalSerial).MapScanCAS(map[string]any{}); err != nil {
		return err
	}
	return s.pruneSnapshots(ctx, stream)
}
func (s snapshots) pruneSnapshots(ctx context.Context, stream string) error {
	// Keep the candidate list bounded; remove retired entries page by page.
	iter := s.db.session.Query(`SELECT sequence FROM m2_stream_snapshot_manifests WHERE stream=?`, stream).WithContext(ctx).Consistency(gocql.LocalQuorum).PageSize(32).Iter()
	var seq int64
	n := 0
	for iter.Scan(&seq) {
		n++
		if n <= storage.SnapshotsRetained {
			continue
		}
		if err := s.db.session.Query(`DELETE FROM m2_stream_snapshot_manifests WHERE stream=? AND sequence=?`, stream, seq).WithContext(ctx).Consistency(gocql.LocalQuorum).Exec(); err != nil {
			_ = iter.Close()
			return err
		}
		// Includes losing digest attempts for this retired sequence. A frozen
		// retired uploader may leave an orphan; it cannot corrupt retained data.
		if err := s.db.session.Query(`DELETE FROM m2_stream_snapshot_parts WHERE stream=? AND sequence=?`, stream, seq).WithContext(ctx).Consistency(gocql.LocalQuorum).Exec(); err != nil {
			_ = iter.Close()
			return err
		}
	}
	return iter.Close()
}

type snapshotEntry struct {
	seq    int64
	chunks int
	size   int64
	digest string
}

func (s snapshots) OpenLatestSnapshot(ctx context.Context, stream string, atMost int64, dir string) (int64, io.ReadCloser, error) {
	iter := s.db.session.Query(`SELECT sequence,chunks,bytes,sha256 FROM m2_stream_snapshot_manifests WHERE stream=? AND sequence<=? LIMIT ?`, stream, atMost, storage.SnapshotsRetained).
		WithContext(ctx).Consistency(gocql.LocalQuorum).PageSize(storage.SnapshotsRetained).Iter()
	var candidates []snapshotEntry
	var e snapshotEntry
	for iter.Scan(&e.seq, &e.chunks, &e.size, &e.digest) {
		candidates = append(candidates, e)
	}
	if err := iter.Close(); err != nil {
		return 0, nil, err
	}
	for _, e := range candidates {
		r, err := s.downloadSnapshot(ctx, stream, e, dir)
		if errors.Is(err, storage.ErrInvalidSnapshot) {
			continue
		}
		if err != nil {
			return 0, nil, err
		}
		return e.seq, r, nil
	}
	return 0, nil, nil
}
func (s snapshots) downloadSnapshot(ctx context.Context, stream string, e snapshotEntry, dir string) (io.ReadCloser, error) {
	sink, err := storage.NewSnapshotSink(dir)
	if err != nil {
		return nil, err
	}
	defer sink.Close()
	iter := s.db.session.Query(`SELECT chunk,data FROM m2_stream_snapshot_parts WHERE stream=? AND sequence=? AND sha256=?`, stream, e.seq, e.digest).
		WithContext(ctx).Consistency(gocql.LocalQuorum).PageSize(2).Iter()
	var index int
	var data []byte
	for iter.Scan(&index, &data) {
		if err := sink.Append(ctx, index, data); err != nil {
			_ = iter.Close()
			return nil, err
		}
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return sink.Finish(e.chunks, e.size, e.digest)
}

var _ storage.StreamSnapshots = snapshots{}
var _ storage.StreamingSnapshots = snapshots{}
