package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/jackc/pgx/v5"
)

type snapshots struct{ db *DB }

func (s snapshots) SaveSnapshot(ctx context.Context, stream string, sequence int64, state []byte) error {
	sum := sha256.Sum256(state)
	if _, err := s.db.Pool.Exec(ctx, `INSERT INTO m2_stream_snapshots (stream,sequence,state,sha256) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
		stream, sequence, state, hex.EncodeToString(sum[:])); err != nil {
		return err
	}
	_, err := s.db.Pool.Exec(ctx, `DELETE FROM m2_stream_snapshots WHERE stream=$1 AND sequence < (
		SELECT min(sequence) FROM (SELECT sequence FROM m2_stream_snapshots WHERE stream=$1 ORDER BY sequence DESC LIMIT $2) kept)`,
		stream, storage.SnapshotsRetained)
	return err
}

func (s snapshots) LatestSnapshot(ctx context.Context, stream string, atMost int64) (int64, []byte, error) {
	rows, err := s.db.Pool.Query(ctx, `SELECT sequence,state,sha256 FROM m2_stream_snapshots WHERE stream=$1 AND sequence<=$2 ORDER BY sequence DESC LIMIT $3`,
		stream, atMost, storage.SnapshotsRetained)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		var state []byte
		var digest string
		if err = rows.Scan(&seq, &state, &digest); err != nil {
			return 0, nil, err
		}
		if sum := sha256.Sum256(state); hex.EncodeToString(sum[:]) == digest {
			return seq, state, nil
		}
	}
	if err = rows.Err(); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, err
	}
	return 0, nil, nil
}

var _ storage.StreamSnapshots = snapshots{}
