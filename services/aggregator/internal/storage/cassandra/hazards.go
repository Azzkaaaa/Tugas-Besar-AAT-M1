package cassandra

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type hazards struct{ db *DB }

func (s hazards) CheckReadiness(ctx context.Context) error {
	var version string
	return s.db.session.Query(`SELECT release_version FROM system.local`).WithContext(ctx).
		Consistency(gocql.One).Scan(&version)
}

// scopeAll is the hazard_buckets scope that indexes buckets of every source.
const scopeAll = "*"

const hazardColumns = `hazard_id, source, source_ref_id, hazard_type, severity,
	area_name, latitude, longitude, occurred_at, ingested_at, attributes, revision`

func scanHazard(scan func(dest ...any) error) (*domain.HazardRecord, error) {
	var h domain.HazardRecord
	var attrs string
	if err := scan(&h.HazardID, &h.Source, &h.SourceRefID, &h.HazardType, &h.Severity,
		&h.AreaName, &h.Latitude, &h.Longitude, &h.OccurredAt, &h.IngestedAt, &attrs, &h.Revision); err != nil {
		return nil, err
	}
	h.OccurredAt = h.OccurredAt.UTC()
	h.IngestedAt = h.IngestedAt.UTC()
	if err := json.Unmarshal([]byte(attrs), &h.Attributes); err != nil || h.Attributes == nil {
		h.Attributes = map[string]any{}
	}
	if h.Revision < 1 {
		h.Revision = 1 // rows written before revisions existed
	}
	return &h, nil
}

// msUTC is t as Cassandra stores it: UTC, millisecond precision.
func msUTC(t time.Time) time.Time {
	return t.UTC().Truncate(time.Millisecond)
}

// stored is a hazards_by_id row: the record's committed revision.
//
// hazards_by_id is the commit point of revisions. Its rows are keyed by the derived
// domain.HazardID, so every writer of a record addresses the same row, and they are only written
// with lightweight transactions, so writers agree on what each revision is (even two leaders
// during an overlap). The other tables (the List row, the bucket index, the outbox) are a
// projection written by a logged batch that may run late, twice, or after a crash not at all.
// Writing revision r again gives the same rows and the same event ID. The List row's revision
// marks a revision as written out; one that is not is written by the next ApplyRevision.
type stored struct {
	rec      *domain.HazardRecord
	bucket   time.Time // bucket of the current List row
	hash     string
	sequence *int64
	// prevBucket/prevOccurredAt locate the List row of the previous revision, which writing this
	// one out deletes when the record moved. Zero for revision 1.
	prevBucket, prevOccurredAt time.Time
}

// maxClaimAttempts bounds retries when concurrent writers keep changing the record.
const maxClaimAttempts = 5

// ApplyRevision implements storage.HazardStore.
func (s hazards) ApplyRevision(ctx context.Context, rev *domain.HazardRecord,
	newEvent func(saved *domain.HazardRecord) (storage.OutboxEvent, error),
) (*domain.HazardRecord, bool, error) {
	return s.applyRevision(ctx, rev, 0, newEvent)
}

func (s hazards) ApplyIngestRevision(ctx context.Context, rev *domain.HazardRecord, sequence int64,
	newEvent func(saved *domain.HazardRecord) (storage.OutboxEvent, error),
) (*domain.HazardRecord, bool, error) {
	return s.applyRevision(ctx, rev, sequence, newEvent)
}

func (s hazards) applyRevision(ctx context.Context, rev *domain.HazardRecord, sequence int64,
	newEvent func(saved *domain.HazardRecord) (storage.OutboxEvent, error),
) (*domain.HazardRecord, bool, error) {
	h := *rev
	h.HazardID = domain.HazardID(rev.Source, rev.SourceRefID)
	h.Attributes = domain.NormalizeAttributes(rev.Attributes)
	h.OccurredAt = msUTC(rev.OccurredAt)
	hash, err := contentHash(&h)
	if err != nil {
		return nil, false, err
	}

	for attempt := 0; attempt < maxClaimAttempts; attempt++ {
		st, err := s.readStored(ctx, h.HazardID, true)
		if err != nil {
			return nil, false, fmt.Errorf("check existing hazard failed: %w", err)
		}

		if st == nil { // new record: revision 1
			if h.IngestedAt.IsZero() {
				h.IngestedAt = time.Now()
			}
			h.IngestedAt = msUTC(h.IngestedAt)
			h.Revision = 1
			ev, err := buildEvent(&h, newEvent)
			if err != nil {
				return nil, false, err
			}
			bucket := s.db.bucketOf(h.OccurredAt)
			applied, err := s.db.session.Query(`INSERT INTO hazards_by_id (`+storedColumns+`, ingest_sequence)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
				append(storedValues(&h, bucket, hash, time.Time{}, time.Time{}), sequence)...).WithContext(ctx).MapScanCAS(map[string]any{})
			if err != nil {
				return nil, false, fmt.Errorf("claim hazard failed: %w", err)
			}
			if !applied {
				continue // another writer created it: re-read
			}
			return s.writeOut(ctx, &h, bucket, time.Time{}, time.Time{}, &ev)
		}

		priorSequence := int64(0)
		if st.sequence != nil {
			priorSequence = *st.sequence
		}
		if sequence > 0 && priorSequence > sequence {
			return st.rec, false, nil
		}
		h.IngestedAt = st.rec.IngestedAt
		if domain.StreamSequence(st.rec) > domain.StreamSequence(&h) {
			return st.rec, false, nil
		}
		written, err := s.writtenOut(ctx, st)
		if err != nil {
			return nil, false, err
		}
		repaired := !written
		if !written {
			// Write the committed revision out before replacing it, so its List row exists for the
			// next revision to move.
			ev, err := buildEvent(st.rec, newEvent)
			if err != nil {
				return nil, false, err
			}
			if _, _, err := s.writeOut(ctx, st.rec, st.bucket, st.prevBucket, st.prevOccurredAt, &ev); err != nil {
				return nil, false, err
			}
		}
		if st.hash == hash || (sequence > 0 && priorSequence == sequence) {
			if sequence > priorSequence {
				applied, err := s.db.session.Query(`UPDATE hazards_by_id SET ingest_sequence = ?
					WHERE hazard_id = ? IF revision = ? AND ingest_sequence = ?`, sequence,
					h.HazardID, st.rec.Revision, st.sequence).WithContext(ctx).MapScanCAS(map[string]any{})
				if err != nil {
					return nil, false, err
				}
				if !applied {
					continue
				}
			}
			return st.rec, repaired, nil
		}

		h.Revision = st.rec.Revision + 1
		ev, err := buildEvent(&h, newEvent)
		if err != nil {
			return nil, false, err
		}
		bucket := s.db.bucketOf(h.OccurredAt)
		applied, err := s.db.session.Query(`UPDATE hazards_by_id SET
				hazard_type = ?, severity = ?, area_name = ?, latitude = ?, longitude = ?, occurred_at = ?,
				attributes = ?, revision = ?, bucket = ?, content_hash = ?, prev_bucket = ?, prev_occurred_at = ?, ingest_sequence = ?
			WHERE hazard_id = ? IF revision = ? AND ingest_sequence = ?`,
			h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude, h.OccurredAt,
			mustJSON(h.Attributes), h.Revision, bucket, hash, st.bucket, st.rec.OccurredAt, max(sequence, priorSequence),
			h.HazardID, st.rec.Revision, st.sequence).WithContext(ctx).MapScanCAS(map[string]any{})
		if err != nil {
			return nil, false, fmt.Errorf("claim hazard revision failed: %w", err)
		}
		if !applied {
			continue
		}
		return s.writeOut(ctx, &h, bucket, st.bucket, st.rec.OccurredAt, &ev)
	}
	return nil, false, fmt.Errorf("hazard %s/%s: kept changing under concurrent writers", h.Source, h.SourceRefID)
}

// buildEvent builds the outbox event of h before anything is claimed, so a failing builder
// leaves the store unchanged.
func buildEvent(h *domain.HazardRecord, newEvent func(*domain.HazardRecord) (storage.OutboxEvent, error)) (storage.OutboxEvent, error) {
	ev, err := newEvent(h)
	if err != nil {
		return storage.OutboxEvent{}, fmt.Errorf("build outbox event: %w", err)
	}
	return ev, nil
}

// storedColumns are the hazards_by_id columns written on insert.
const storedColumns = hazardColumns + `, bucket, content_hash, prev_bucket, prev_occurred_at`

func storedValues(h *domain.HazardRecord, bucket time.Time, hash string, prevBucket, prevOccurredAt time.Time) []any {
	return []any{h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity,
		h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, mustJSON(h.Attributes), h.Revision,
		bucket, hash, nullableTime(prevBucket), nullableTime(prevOccurredAt)}
}

func mustJSON(attrs map[string]any) string {
	b, err := json.Marshal(attrs)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// readStored reads a hazards_by_id row (at serial consistency when serial, so it sees every
// committed lightweight transaction), or nil when there is none.
func (s hazards) readStored(ctx context.Context, hazardID string, serial bool) (*stored, error) {
	var st stored
	var hash *string
	q := s.db.session.Query(`SELECT `+storedColumns+`, ingest_sequence FROM hazards_by_id WHERE hazard_id = ?`, hazardID).WithContext(ctx)
	if serial {
		q = q.Consistency(gocql.LocalSerial)
	}
	rec, err := scanHazard(func(dest ...any) error {
		return q.Scan(append(dest, &st.bucket, &hash, &st.prevBucket, &st.prevOccurredAt, &st.sequence)...)
	})
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	st.rec = rec
	st.prevBucket, st.prevOccurredAt = utcOrZero(st.prevBucket), utcOrZero(st.prevOccurredAt)
	if st.bucket = utcOrZero(st.bucket); st.bucket.IsZero() {
		st.bucket = s.db.bucketOf(rec.OccurredAt)
	}
	if hash != nil && *hash != "" {
		st.hash = *hash
	} else if st.hash, err = contentHash(rec); err != nil {
		return nil, err
	}
	return &st, nil
}

// writtenOut reports whether the List row of st's revision exists.
func (s hazards) writtenOut(ctx context.Context, st *stored) (bool, error) {
	var revision int
	err := s.db.session.Query(`SELECT revision FROM hazards_by_bucket WHERE bucket = ? AND occurred_at = ? AND hazard_id = ?`,
		st.bucket, st.rec.OccurredAt, st.rec.HazardID).WithContext(ctx).Scan(&revision)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check hazard list row failed: %w", err)
	}
	return revision >= st.rec.Revision, nil
}

// writeOut writes the projection of h's committed revision in one logged batch: the List row
// (moving it from prev when the record moved), the bucket index, and ev when not nil.
func (s hazards) writeOut(ctx context.Context, h *domain.HazardRecord, bucket, prevBucket, prevOccurredAt time.Time,
	ev *storage.OutboxEvent,
) (*domain.HazardRecord, bool, error) {
	b := s.db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	s.addProjection(b, h, bucket, prevBucket, prevOccurredAt)
	if ev != nil {
		addOutboxEnqueue(b, *ev, time.Now())
	}
	if err := b.Exec(); err != nil {
		return nil, false, fmt.Errorf("write hazard failed: %w", err)
	}
	return h, true, nil
}

// addProjection adds the List row of h, the removal of the previous one if it moved, and the
// bucket index entries to b.
func (s hazards) addProjection(b *gocql.Batch, h *domain.HazardRecord, bucket, prevBucket, prevOccurredAt time.Time) {
	if !prevBucket.IsZero() && (!prevBucket.Equal(bucket) || !prevOccurredAt.Equal(h.OccurredAt)) {
		b.Query(`DELETE FROM hazards_by_bucket WHERE bucket = ? AND occurred_at = ? AND hazard_id = ?`,
			prevBucket, prevOccurredAt, h.HazardID)
	}
	b.Query(`INSERT INTO hazards_by_bucket (bucket, occurred_at, hazard_id, source, source_ref_id, hazard_type,
			severity, area_name, latitude, longitude, ingested_at, attributes, revision)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		bucket, h.OccurredAt, h.HazardID, h.Source, h.SourceRefID, h.HazardType,
		h.Severity, h.AreaName, h.Latitude, h.Longitude, h.IngestedAt, mustJSON(h.Attributes), h.Revision)
	b.Query(`INSERT INTO hazard_buckets (scope, bucket) VALUES (?, ?)`, scopeAll, bucket)
	b.Query(`INSERT INTO hazard_buckets (scope, bucket) VALUES (?, ?)`, h.Source, bucket)
}

// contentHash identifies the content a revision can change (see sameContent in the Postgres
// backend): sha256 of its JSON, whose map keys encoding/json sorts. Attributes must be normalized.
func contentHash(h *domain.HazardRecord) (string, error) {
	b, err := json.Marshal(struct {
		HazardType string         `json:"t"`
		Severity   string         `json:"s"`
		AreaName   string         `json:"a"`
		Latitude   float64        `json:"la"`
		Longitude  float64        `json:"lo"`
		OccurredAt int64          `json:"o"`
		Attributes map[string]any `json:"at"`
	}{h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude, msUTC(h.OccurredAt).UnixMilli(), h.Attributes})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
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
	Restart    bool      `json:"r,omitempty"`
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
	if c.Restart && c.OccurredAt.IsZero() && c.HazardID == "" {
		return nil, nil
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

	return collectBuckets(ctx, buckets, scope, page, func(bucket time.Time, want int) ([]*domain.HazardRecord, error) {
		unitCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		return s.readBucket(unitCtx, bucket, q, since, until, cursor, want)
	}), nil
}

type bucketIterator interface {
	Scan(...any) bool
	Close() error
}

// Preserve successful ranges even when a different range fails. A partial page
// deliberately retries its incoming cursor: advancing it could permanently hide
// records in an unavailable newer bucket once that bucket recovers.
func collectBuckets(ctx context.Context, buckets bucketIterator, scope string, page storage.PageRequest,
	read func(time.Time, int) ([]*domain.HazardRecord, error),
) storage.HazardPage {
	out := storage.HazardPage{}
	var bucket time.Time
	for len(out.Items) <= page.Size && ctx.Err() == nil && buckets.Scan(&bucket) {
		items, err := read(bucket, page.Size+1-len(out.Items))
		out.Items = append(out.Items, items...)
		if err != nil {
			out.UnavailableUnits = append(out.UnavailableUnits, "hazard-bucket:"+bucket.UTC().Format(time.RFC3339Nano))
		}
	}
	if err := buckets.Close(); err != nil || ctx.Err() != nil {
		out.UnavailableUnits = append(out.UnavailableUnits, "hazard-index:"+scope)
	}
	out.Partial = len(out.UnavailableUnits) > 0
	if len(out.Items) > page.Size {
		out.NextToken = encodeCursor(out.Items[page.Size-1])
		out.Items = out.Items[:page.Size]
	}
	if out.Partial {
		out.NextToken = page.Token
		if out.NextToken == "" {
			b, _ := json.Marshal(pageCursor{Restart: true})
			out.NextToken = base64.RawURLEncoding.EncodeToString(b)
		}
	}
	return out
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
		if !domain.Withdrawn(h) {
			out = append(out, h)
		}
	}
	if err := scanner.Err(); err != nil {
		return out, fmt.Errorf("failed reading hazard bucket: %w", err)
	}
	return out, nil
}
