package postgres

import (
	"bytes"
	"context"
	"errors"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/jackc/pgx/v5"
	"io"
	"time"
)

type snapshots struct{ db *DB }

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
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	tx, err := s.db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	// Serialize publication/retention for this stream, independently of ingestion.
	// Server timeouts also release a connection whose client has frozen.
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout = '30s'; SET LOCAL idle_in_transaction_session_timeout = '30s'; SET LOCAL transaction_timeout = '120s'`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, stream); err != nil {
		return err
	}
	chunks := int((size + storage.SnapshotChunkBytes - 1) / storage.SnapshotChunkBytes)
	if chunks == 0 {
		chunks = 1
	}
	tag, err := tx.Exec(ctx, `INSERT INTO m2_stream_snapshot_manifests (stream,sequence,chunks,bytes,sha256) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, stream, sequence, chunks, size, digest)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 0 {
		_, err = storage.UploadSnapshotChunks(ctx, source, size, digest, func(index int, data []byte) error {
			_, err := tx.Exec(ctx, `INSERT INTO m2_stream_snapshot_parts (stream,sequence,chunk,data) VALUES ($1,$2,$3,$4)`, stream, sequence, index, data)
			return err
		})
		if err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM m2_stream_snapshot_manifests WHERE stream=$1 AND sequence < (
  SELECT min(sequence) FROM (SELECT sequence FROM m2_stream_snapshot_manifests WHERE stream=$1 ORDER BY sequence DESC LIMIT $2) kept)`, stream, storage.SnapshotsRetained); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type snapshotEntry struct {
	seq    int64
	chunks int
	size   int64
	digest string
}

func (s snapshots) OpenLatestSnapshot(ctx context.Context, stream string, atMost int64, dir string) (int64, io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	// Retention may run while this download is in progress. A repeatable read
	// keeps the manifest and its chunks from different generations apart.
	tx, err := s.db.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `SELECT sequence,chunks,bytes,sha256 FROM m2_stream_snapshot_manifests WHERE stream=$1 AND sequence<=$2 ORDER BY sequence DESC LIMIT $3`, stream, atMost, storage.SnapshotsRetained)
	if err != nil {
		return 0, nil, err
	}
	var candidates []snapshotEntry
	for rows.Next() {
		var e snapshotEntry
		if err := rows.Scan(&e.seq, &e.chunks, &e.size, &e.digest); err != nil {
			rows.Close()
			return 0, nil, err
		}
		candidates = append(candidates, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, nil, err
	}
	for _, e := range candidates {
		r, err := s.downloadSnapshot(ctx, tx, stream, e, dir)
		if errors.Is(err, storage.ErrInvalidSnapshot) {
			continue
		}
		if err != nil {
			return 0, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			r.Close()
			return 0, nil, err
		}
		return e.seq, r, nil
	}
	return 0, nil, nil
}
func (s snapshots) downloadSnapshot(ctx context.Context, tx pgx.Tx, stream string, e snapshotEntry, dir string) (io.ReadCloser, error) {
	sink, err := storage.NewSnapshotSink(dir)
	if err != nil {
		return nil, err
	}
	defer sink.Close()
	rows, err := tx.Query(ctx, `SELECT chunk,data FROM m2_stream_snapshot_parts WHERE stream=$1 AND sequence=$2 ORDER BY chunk`, stream, e.seq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var index int
		var data []byte
		if err := rows.Scan(&index, &data); err != nil {
			return nil, err
		}
		if err := sink.Append(ctx, index, data); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sink.Finish(e.chunks, e.size, e.digest)
}

var _ storage.StreamSnapshots = snapshots{}
var _ storage.StreamingSnapshots = snapshots{}
