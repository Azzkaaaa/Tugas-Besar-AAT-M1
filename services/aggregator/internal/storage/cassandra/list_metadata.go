package cassandra

import (
	"context"
	"errors"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type listMetadata struct {
	bytes  int
	bucket time.Time
}

func (s hazards) readProjection(ctx context.Context, header *domain.HazardRecord, bucket time.Time) (*domain.HazardRecord, error) {
	h, err := scanHazard(s.db.session.Query(`SELECT `+hazardColumns+` FROM hazards_by_bucket_shard
		WHERE bucket=? AND shard=? AND occurred_at=? AND hazard_id=?`, bucket,
		projectionShard(header.HazardID), header.OccurredAt, header.HazardID).WithContext(ctx).Scan)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	return h, err
}

// Missing/stale cache entries are read singly and filled only while the row's
// revision is unchanged. This writes derived metadata only; it cannot modify
// canonical data, authority, outbox or the projection completion revision.
func (s hazards) resolveListMetadata(ctx context.Context, header *domain.HazardRecord, bucket time.Time, revision *int,
	slots chan struct{},
) (*domain.HazardRecord, int, error) {
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
	defer func() { <-slots }()
	h, err := s.readProjection(ctx, header, bucket)
	if err != nil || h == nil {
		return h, 0, err
	}
	size := domain.FullProjectionSize(h)
	if revision != nil && h.Revision == header.Revision {
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		// Cache failure is harmless. A newer revision or a rolling old writer
		// invalidates this cache automatically on the next metadata read.
		_, _ = s.db.session.Query(`UPDATE hazards_by_bucket_shard SET list_active=?,list_revision=?,list_wire_bytes=?
			WHERE bucket=? AND shard=? AND occurred_at=? AND hazard_id=? IF revision=?`,
			!domain.Withdrawn(h), *revision, int64(size), bucket, projectionShard(header.HazardID),
			header.OccurredAt, header.HazardID, *revision).WithContext(cacheCtx).MapScanCAS(map[string]any{})
	}
	return h, size, nil
}
