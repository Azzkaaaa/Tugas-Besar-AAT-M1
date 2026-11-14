package cassandra

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

type hazards struct{ db *DB }

// scopeAll is the hazard_buckets scope that indexes buckets of every source.
const scopeAll = "*"

const hazardColumns = `hazard_id, source, source_ref_id, hazard_type, severity,
	area_name, latitude, longitude, occurred_at, ingested_at, attributes`

func scanHazard(scan func(dest ...any) error) (*domain.HazardRecord, error) {
	var h domain.HazardRecord
	var attrs string
	if err := scan(&h.HazardID, &h.Source, &h.SourceRefID, &h.HazardType, &h.Severity,
		&h.AreaName, &h.Latitude, &h.Longitude, &h.OccurredAt, &h.IngestedAt, &attrs); err != nil {
		return nil, err
	}
	h.OccurredAt = h.OccurredAt.UTC()
	h.IngestedAt = h.IngestedAt.UTC()
	if err := json.Unmarshal([]byte(attrs), &h.Attributes); err != nil || h.Attributes == nil {
		h.Attributes = map[string]any{}
	}
	return &h, nil
}

// msUTC is t as Cassandra stores it: UTC, millisecond precision.
func msUTC(t time.Time) time.Time {
	return t.UTC().Truncate(time.Millisecond)
}

// ApplyRevision claims the identity row with a lightweight transaction when the record is new,
// then writes every hazard table and the outbox event in one logged batch, which Cassandra
// applies completely or not at all (eventually, on every replica).
func (s hazards) ApplyRevision(ctx context.Context, rev *domain.HazardRecord,
	newEvent func(saved *domain.HazardRecord) (storage.OutboxEvent, error),
) (*domain.HazardRecord, bool, error) {
	h := *rev
	h.Attributes = domain.NormalizeAttributes(rev.Attributes)
	h.OccurredAt = msUTC(rev.OccurredAt)

	id, ingestedAt, err := s.claimIdentity(ctx, &h)
	if err != nil {
		return nil, false, err
	}
	h.HazardID, h.IngestedAt = id, ingestedAt

	// A missing row means a new record, or a claim whose batch never ran: write it either way.
	existing, oldBucket, err := s.getStored(ctx, id)
	if err != nil {
		return nil, false, fmt.Errorf("check existing hazard failed: %w", err)
	}
	if existing != nil && sameContent(existing, &h) {
		return &h, false, nil
	}

	ev, err := newEvent(&h)
	if err != nil {
		return nil, false, fmt.Errorf("build outbox event: %w", err)
	}
	attrJSON, err := json.Marshal(h.Attributes)
	if err != nil {
		return nil, false, err
	}

	b := s.db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	s.addHazardRows(b, &h, existing, oldBucket, string(attrJSON))
	addOutboxEnqueue(b, ev, time.Now())
	if err := b.Exec(); err != nil {
		return nil, false, fmt.Errorf("write hazard failed: %w", err)
	}
	return &h, true, nil
}

// addHazardRows adds the writes that store h to b: its row by id, its List row and bucket index
// entries, and the removal of its previous List row when it moved (existing and oldBucket are
// the stored record and the bucket of its List row, or nil and zero for a new record).
func (s hazards) addHazardRows(b *gocql.Batch, h *domain.HazardRecord, existing *domain.HazardRecord, oldBucket time.Time, attrJSON string) {
	bucket := s.db.bucketOf(h.OccurredAt)
	b.Query(`INSERT INTO hazards_by_id (`+hazardColumns+`, bucket) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity,
		h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, attrJSON, bucket)
	if existing != nil && (!existing.OccurredAt.Equal(h.OccurredAt) || !oldBucket.Equal(bucket)) {
		b.Query(`DELETE FROM hazards_by_bucket WHERE bucket = ? AND occurred_at = ? AND hazard_id = ?`,
			oldBucket, existing.OccurredAt, h.HazardID)
	}
	b.Query(`INSERT INTO hazards_by_bucket (bucket, occurred_at, hazard_id, source, source_ref_id, hazard_type,
			severity, area_name, latitude, longitude, ingested_at, attributes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		bucket, h.OccurredAt, h.HazardID, h.Source, h.SourceRefID, h.HazardType,
		h.Severity, h.AreaName, h.Latitude, h.Longitude, h.IngestedAt, attrJSON)
	b.Query(`INSERT INTO hazard_buckets (scope, bucket) VALUES (?, ?)`, scopeAll, bucket)
	b.Query(`INSERT INTO hazard_buckets (scope, bucket) VALUES (?, ?)`, h.Source, bucket)
}

// claimIdentity returns the hazard_id and ingested_at of (source, source_ref_id), creating them
// when the pair is new. A lost race returns the winner's identity.
func (s hazards) claimIdentity(ctx context.Context, h *domain.HazardRecord) (string, time.Time, error) {
	var id string
	var ingestedAt time.Time
	err := s.db.session.Query(`SELECT hazard_id, ingested_at FROM hazard_by_source_ref WHERE source = ? AND source_ref_id = ?`,
		h.Source, h.SourceRefID).WithContext(ctx).Scan(&id, &ingestedAt)
	if err == nil {
		return id, ingestedAt.UTC(), nil
	}
	if !errors.Is(err, gocql.ErrNotFound) {
		return "", time.Time{}, fmt.Errorf("check existing hazard failed: %w", err)
	}

	id = h.HazardID
	if id == "" {
		id = "hz-" + uuid.New().String()
	}
	ingestedAt = h.IngestedAt
	if ingestedAt.IsZero() {
		ingestedAt = time.Now()
	}
	ingestedAt = msUTC(ingestedAt)
	existing := map[string]any{}
	applied, err := s.db.session.Query(`INSERT INTO hazard_by_source_ref (source, source_ref_id, hazard_id, ingested_at)
		VALUES (?, ?, ?, ?) IF NOT EXISTS`, h.Source, h.SourceRefID, id, ingestedAt).WithContext(ctx).MapScanCAS(existing)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("claim hazard identity failed: %w", err)
	}
	if !applied {
		id, _ = existing["hazard_id"].(string)
		ingestedAt, _ = existing["ingested_at"].(time.Time)
	}
	return id, ingestedAt.UTC(), nil
}

// sameContent compares the fields a source revision can change; attributes must be normalized.
func sameContent(a, b *domain.HazardRecord) bool {
	return a.HazardType == b.HazardType &&
		a.Severity == b.Severity &&
		a.AreaName == b.AreaName &&
		a.Latitude == b.Latitude &&
		a.Longitude == b.Longitude &&
		a.OccurredAt.Equal(b.OccurredAt) &&
		reflect.DeepEqual(a.Attributes, b.Attributes)
}

// getStored returns the stored record and the bucket of its List row (computed for rows written
// before hazards_by_id.bucket existed), or nil when there is none.
func (s hazards) getStored(ctx context.Context, hazardID string) (*domain.HazardRecord, time.Time, error) {
	var bucket time.Time
	h, err := scanHazard(func(dest ...any) error {
		return s.db.session.Query(`SELECT `+hazardColumns+`, bucket FROM hazards_by_id WHERE hazard_id = ?`, hazardID).
			WithContext(ctx).Scan(append(dest, &bucket)...)
	})
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, time.Time{}, nil
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	if bucket = utcOrZero(bucket); bucket.IsZero() {
		bucket = s.db.bucketOf(h.OccurredAt)
	}
	return h, bucket, nil
}

func (s hazards) Get(ctx context.Context, hazardID string) (*domain.HazardRecord, error) {
	h, err := scanHazard(s.db.session.Query(`SELECT `+hazardColumns+` FROM hazards_by_id WHERE hazard_id = ?`, hazardID).
		WithContext(ctx).Scan)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	return h, err
}

func (s hazards) HasData(ctx context.Context, source string) (bool, error) {
	var bucket time.Time
	err := s.db.session.Query(`SELECT bucket FROM hazard_buckets WHERE scope = ? LIMIT 1`, source).WithContext(ctx).Scan(&bucket)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// pageCursor is the position after the last record of a page.
type pageCursor struct {
	OccurredAt time.Time `json:"o"`
	HazardID   string    `json:"h"`
}

func encodeCursor(h *domain.HazardRecord) string {
	b, _ := json.Marshal(pageCursor{OccurredAt: h.OccurredAt, HazardID: h.HazardID})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (*pageCursor, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var c pageCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.OccurredAt.IsZero() || c.HazardID == "" {
		return nil, errors.New("incomplete cursor")
	}
	return &c, nil
}

// List walks the non-empty buckets of the query's scope from newest to oldest, reading each
// bucket's rows newest first, until it has a page. Source/type/severity are filtered in memory:
// a selective filter over a long history reads many buckets.
func (s hazards) List(ctx context.Context, q storage.HazardQuery, page storage.PageRequest) (storage.HazardPage, error) {
	cursor, err := decodeCursor(page.Token)
	if err != nil {
		return storage.HazardPage{}, fmt.Errorf("%w: %v", storage.ErrInvalidPageToken, err)
	}
	since, until := msUTC(q.Since), msUTC(q.Until) // zero stays zero

	scope := scopeAll
	if q.Source != "" {
		scope = q.Source
	}

	// Bucket range, newest first. The cursor's bucket is the newest one still to read.
	stmt := `SELECT bucket FROM hazard_buckets WHERE scope = ?`
	args := []any{scope}
	switch {
	case cursor != nil:
		stmt += ` AND bucket <= ?`
		args = append(args, s.db.bucketOf(cursor.OccurredAt))
	case !until.IsZero():
		// Until is exclusive: a bucket starting at Until holds nothing before it.
		stmt += ` AND bucket <= ?`
		args = append(args, s.db.bucketOf(until.Add(-time.Millisecond)))
	}
	if !since.IsZero() {
		stmt += ` AND bucket >= ?`
		args = append(args, s.db.bucketOf(since))
	}
	buckets := s.db.session.Query(stmt, args...).WithContext(ctx).PageSize(200).Iter()

	var out storage.HazardPage
	var bucket time.Time
	for len(out.Items) <= page.Size && buckets.Scan(&bucket) {
		items, err := s.readBucket(ctx, bucket, q, since, until, cursor, page.Size+1-len(out.Items))
		if err != nil {
			_ = buckets.Close()
			return storage.HazardPage{}, err
		}
		out.Items = append(out.Items, items...)
	}
	if err := buckets.Close(); err != nil {
		return storage.HazardPage{}, fmt.Errorf("failed listing hazard buckets: %w", err)
	}

	if len(out.Items) > page.Size {
		out.NextToken = encodeCursor(out.Items[page.Size-1])
		out.Items = out.Items[:page.Size]
	}
	return out, nil
}

// readBucket returns up to want matching rows of one bucket, in List order.
func (s hazards) readBucket(ctx context.Context, bucket time.Time, q storage.HazardQuery, since, until time.Time,
	cursor *pageCursor, want int,
) ([]*domain.HazardRecord, error) {
	stmt := `SELECT ` + hazardColumns + ` FROM hazards_by_bucket WHERE bucket = ?`
	args := []any{bucket}
	// CQL allows one upper bound: the cursor's (inclusive; rows at exactly its time are filtered
	// by hazard_id below) or Until's (exclusive), whichever is tighter.
	switch {
	case cursor != nil && (until.IsZero() || cursor.OccurredAt.Before(until)):
		stmt += ` AND occurred_at <= ?`
		args = append(args, cursor.OccurredAt)
	case !until.IsZero():
		stmt += ` AND occurred_at < ?`
		args = append(args, until)
	}
	if !since.IsZero() {
		stmt += ` AND occurred_at >= ?`
		args = append(args, since)
	}
	iter := s.db.session.Query(stmt, args...).WithContext(ctx).PageSize(max(want, 50)).Iter()
	scanner := iter.Scanner()

	var out []*domain.HazardRecord
	for len(out) < want && scanner.Next() {
		h, err := scanHazard(scanner.Scan)
		if err != nil {
			_ = scanner.Err()
			return nil, err
		}
		if cursor != nil && h.OccurredAt.Equal(cursor.OccurredAt) && h.HazardID <= cursor.HazardID {
			continue
		}
		if (q.Source != "" && h.Source != q.Source) ||
			(q.HazardType != "" && h.HazardType != q.HazardType) ||
			(q.Severity != "" && h.Severity != q.Severity) {
			continue
		}
		out = append(out, h)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed reading hazard bucket: %w", err)
	}
	return out, nil
}
