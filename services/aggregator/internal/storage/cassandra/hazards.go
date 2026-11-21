package cassandra

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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

// stored is a canonical row: the record's committed revision.
//
// canonical_events is the commit point of revisions after migration 0011. A hash shard and
// derived domain.HazardID make every writer address the same row. Conditional batches check
// the source authority and revision in the same partition. Legacy hazards_by_id rows remain
// readable until adoption. The other tables (the List row, the bucket index, the outbox) are a
// projection written by a logged batch that may run late, twice, or after a crash not at all.
// Writing revision r again gives the same rows and the same event ID. The List row's revision
// marks a revision as written out; one that is not is written by the next ApplyRevision.
type stored struct {
	legacy   bool
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
		if st != nil && st.legacy && s.db.fencedSchema {
			if err := s.adoptLegacy(ctx, st); err != nil {
				return nil, false, err
			}
			continue
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
			applied, err := s.insertStored(ctx, &h, bucket, hash, time.Time{}, time.Time{}, sequence, ev.TraceContext)
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
			if err := s.db.ensureCritical(ctx, st.rec); err != nil {
				return nil, false, err
			}
			if sequence > priorSequence {
				b := s.db.guardBatch(ctx, canonicalGuard(h.HazardID))
				b.Query(`UPDATE canonical_events SET ingest_sequence=? WHERE shard=? AND hazard_id=? IF revision=? AND ingest_sequence=?`, sequence, auditShard(h.HazardID), h.HazardID, st.rec.Revision, st.sequence)
				applied, err := s.db.guardedCAS(ctx, canonicalGuard(h.HazardID), b)
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
		b := s.db.guardBatch(ctx, canonicalGuard(h.HazardID))
		b.Query(`UPDATE canonical_events SET
				hazard_type = ?, severity = ?, area_name = ?, latitude = ?, longitude = ?, occurred_at = ?,
				attributes = ?, revision = ?, bucket = ?, content_hash = ?, prev_bucket = ?, prev_occurred_at = ?, ingest_sequence = ?,
				summary_active = ?, summary_revision = ?
			WHERE shard=? AND hazard_id = ? IF revision = ? AND ingest_sequence = ?`,
			h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude, h.OccurredAt,
			mustJSON(h.Attributes), h.Revision, bucket, hash, st.bucket, st.rec.OccurredAt, max(sequence, priorSequence),
			!domain.Withdrawn(&h), h.Revision,
			auditShard(h.HazardID), h.HazardID, st.rec.Revision, st.sequence)
		if err := s.db.addCriticalEvent(ctx, b, &h, ev.TraceContext); err != nil {
			return nil, false, err
		}
		applied, err := s.db.guardedCAS(ctx, canonicalGuard(h.HazardID), b)
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

// storedColumns are the canonical payload columns shared with the legacy table.
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

// readStored reads the current canonical row, falling back to the legacy layout. Serial
// consistency observes committed lightweight transactions. Absence returns nil.
func (s hazards) readStored(ctx context.Context, hazardID string, serial bool) (*stored, error) {
	if criticalInternalKey(hazardID) {
		return nil, nil
	}
	if s.db.fencedSchema {
		st, err := s.readStoredTable(ctx, hazardID, serial, false)
		if st != nil || err != nil {
			return st, err
		}
	}
	return s.readStoredTable(ctx, hazardID, serial, true)
}
func (s hazards) readStoredTable(ctx context.Context, hazardID string, serial, legacy bool) (*stored, error) {
	var st stored
	st.legacy = legacy
	var hash *string
	cols := storedColumns + `, ingest_sequence`
	if legacy && !s.db.fencedSchema {
		cols = storedColumns
	}
	stmt := `SELECT ` + cols + ` FROM canonical_events WHERE shard=? AND hazard_id=?`
	args := []any{auditShard(hazardID), hazardID}
	if legacy {
		stmt = `SELECT ` + cols + ` FROM hazards_by_id WHERE hazard_id=?`
		args = []any{hazardID}
	}
	q := s.db.session.Query(stmt, args...).WithContext(ctx)
	if serial {
		q = q.Consistency(gocql.LocalSerial)
	}
	rec, err := scanHazard(func(dest ...any) error {
		extra := []any{&st.bucket, &hash, &st.prevBucket, &st.prevOccurredAt}
		if !legacy || s.db.fencedSchema {
			extra = append(extra, &st.sequence)
		}
		return q.Scan(append(dest, extra...)...)
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
	err := s.db.session.Query(`SELECT revision FROM hazards_by_bucket_shard WHERE bucket = ? AND shard = ? AND occurred_at = ? AND hazard_id = ?`,
		st.bucket, projectionShard(st.rec.HazardID), st.rec.OccurredAt, st.rec.HazardID).WithContext(ctx).Scan(&revision)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check hazard list row failed: %w", err)
	}
	return revision >= st.rec.Revision, nil
}

// writeOut applies the derived effects of h's committed revision. Small writes
// share a logged batch. Large writes use ordered single-partition mutations,
// with the List revision last as the repair/completion marker.
func (s hazards) writeOut(ctx context.Context, h *domain.HazardRecord, bucket, prevBucket, prevOccurredAt time.Time,
	ev *storage.OutboxEvent,
) (*domain.HazardRecord, bool, error) {
	b := s.db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	s.addProjectionMetadata(b, h, bucket, prevBucket, prevOccurredAt)
	if ev != nil {
		addOutboxEnqueue(b, *ev, time.Now())
	}
	// writtenOut uses this row's revision as the completion marker. It must
	// follow BOTH durable outbox copies when a large write is split. A crash
	// before it leaves canonical state repairable at the same event/revision.
	s.addProjectionRow(b, h, bucket)
	if err := s.db.execRecoverableBatch(ctx, b); err != nil {
		return nil, false, fmt.Errorf("write hazard failed: %w", err)
	}
	return h, true, nil
}

// addProjection adds the List row of h, the removal of the previous one if it moved, and the
// bucket index entries to b.
func (s hazards) addProjection(b *gocql.Batch, h *domain.HazardRecord, bucket, prevBucket, prevOccurredAt time.Time) {
	s.addProjectionMetadata(b, h, bucket, prevBucket, prevOccurredAt)
	s.addProjectionRow(b, h, bucket)
}

func (s hazards) addProjectionMetadata(b *gocql.Batch, h *domain.HazardRecord, bucket, prevBucket, prevOccurredAt time.Time) {
	if s.db.fencedSchema {
		addAuditProjection(b, h)
		addOrderedListProjection(b, h, bucket, prevOccurredAt)
	}
	if !prevBucket.IsZero() && (!prevBucket.Equal(bucket) || !prevOccurredAt.Equal(h.OccurredAt)) {
		b.Query(`DELETE FROM hazards_by_bucket_shard WHERE bucket = ? AND shard = ? AND occurred_at = ? AND hazard_id = ?`,
			prevBucket, projectionShard(h.HazardID), prevOccurredAt, h.HazardID)
	}
	b.Query(`INSERT INTO hazard_buckets (scope, bucket) VALUES (?, ?)`, scopeAll, bucket)
	b.Query(`INSERT INTO hazard_buckets (scope, bucket) VALUES (?, ?)`, h.Source, bucket)
}

func (s hazards) addProjectionRow(b *gocql.Batch, h *domain.HazardRecord, bucket time.Time) {
	b.Query(`INSERT INTO hazards_by_bucket_shard (bucket, shard, occurred_at, hazard_id, source, source_ref_id, hazard_type,
			severity, area_name, latitude, longitude, ingested_at, attributes, revision, list_active, list_revision, list_wire_bytes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		bucket, projectionShard(h.HazardID), h.OccurredAt, h.HazardID, h.Source, h.SourceRefID, h.HazardType,
		h.Severity, h.AreaName, h.Latitude, h.Longitude, h.IngestedAt, mustJSON(h.Attributes), h.Revision,
		!domain.Withdrawn(h), h.Revision, int64(domain.FullProjectionSize(h)))
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
	st, err := s.readStored(ctx, hazardID, false)
	if err != nil || st == nil {
		return nil, err
	}
	return st.rec, nil
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
	page.Size = storage.HazardPageSize(page.Size)
	cursor, err := decodeCursor(page.Token)
	if err != nil {
		return storage.HazardPage{}, fmt.Errorf("%w: %v", storage.ErrInvalidPageToken, err)
	}
	if s.db.orderedListReady(ctx) {
		return s.listOrdered(ctx, q, page, cursor)
	}
	since, until := msUTC(q.Since), msUTC(q.Until) // zero stays zero
	if !q.Until.IsZero() && !q.Until.Equal(until) {
		until = until.Add(time.Millisecond)
	}

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

	// Only headers are retained for fanout/merge. Legacy fallbacks share one
	// slot across every Get/List on this DB, including all eight shard readers.
	var sizes sync.Map
	out := collectBuckets(ctx, buckets, scope, page, func(bucket time.Time, want int) ([]*domain.HazardRecord, error) {
		unitCtx, cancel := context.WithTimeout(ctx, readUnitTimeout)
		defer cancel()
		remember := func(h *domain.HazardRecord, n int) { sizes.Store(h, listMetadata{bytes: n, bucket: bucket}) }
		return s.readBucket(unitCtx, bucket, q, since, until, cursor, want, remember)
	})
	return s.loadListPage(ctx, q, page, out, &sizes)
}

func (s hazards) loadListPage(ctx context.Context, q storage.HazardQuery, page storage.PageRequest, out storage.HazardPage, sizes *sync.Map) (storage.HazardPage, error) {
	candidates := make([]storage.HazardCandidate, len(out.Items))
	for i, h := range out.Items {
		n, _ := sizes.Load(h)
		metadata, _ := n.(listMetadata)
		candidates[i] = storage.HazardCandidate{Header: h, FullBytes: metadata.bytes}
	}
	items, consumed, err := storage.LoadHazardPayloads(ctx, q, candidates, func(h *domain.HazardRecord) (*domain.HazardRecord, error) {
		n, _ := sizes.Load(h)
		return s.readProjection(ctx, h, n.(listMetadata).bucket)
	})
	out.Items = items
	if consumed > 0 && consumed < len(candidates) && !out.Partial {
		out.NextToken = encodeCursor(candidates[consumed-1].Header)
	}
	if timeBudgetShortPage(ctx, out.Partial, consumed, err) {
		// The index range was complete and items[:consumed] is an intact ordered
		// prefix; continue after it exactly like a byte-budget short page. The
		// unhydrated candidates follow the cursor, so nothing is skipped.
		out.NextToken = encodeCursor(candidates[consumed-1].Header)
		return out, nil
	}
	if err != nil {
		out.Partial = true
		out.UnavailableUnits = append(out.UnavailableUnits, "hazard-payload")
		out.NextToken = page.Token
		if out.NextToken == "" {
			out.NextToken = restartCursor()
		}
		return out, err
	}
	return out, nil
}

// timeBudgetShortPage reports whether a FULL hydration that stopped only because
// the caller's time budget ran out may be served as a short complete page. Any
// other failure, an incomplete index range, or no progress keeps the explicit
// partial page that retries its incoming cursor.
func timeBudgetShortPage(ctx context.Context, indexPartial bool, consumed int, err error) bool {
	if err == nil || indexPartial || consumed == 0 {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded)
}

func restartCursor() string {
	b, _ := json.Marshal(pageCursor{Restart: true})
	return base64.RawURLEncoding.EncodeToString(b)
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
			out.UnavailableUnits = append(out.UnavailableUnits, bucketUnits(bucket, err)...)
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
			out.NextToken = restartCursor()
		}
	}
	return out
}

// readBucket returns up to want matching rows of one bucket, in List order,
// merged from every shard partition of the bucket.
func (s hazards) readBucket(ctx context.Context, bucket time.Time, q storage.HazardQuery, since, until time.Time,
	cursor *pageCursor, want int, remember func(*domain.HazardRecord, int),
) ([]*domain.HazardRecord, error) {
	return readBucketShards(ctx, want, func(ctx context.Context, shard int) ([]*domain.HazardRecord, error) {
		return s.readShard(ctx, bucket, shard, q, since, until, cursor, want, remember)
	})
}

// readShard returns up to want matching rows of one bucket shard, in List order.
func (s hazards) readShard(ctx context.Context, bucket time.Time, shard int, q storage.HazardQuery, since, until time.Time,
	cursor *pageCursor, want int, remember func(*domain.HazardRecord, int),
) ([]*domain.HazardRecord, error) {
	stmt := `SELECT hazard_id,source,hazard_type,severity,area_name,occurred_at,ingested_at,revision,
		list_active,list_revision,list_wire_bytes FROM hazards_by_bucket_shard WHERE bucket = ? AND shard = ?`
	args := []any{bucket, shard}
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
	iter := s.db.session.Query(stmt, args...).WithContext(ctx).PageSize(min(want, 64)).Iter()
	scanner := iter.Scanner()

	var out []*domain.HazardRecord
	for len(out) < want && ctx.Err() == nil && scanner.Next() {
		var h domain.HazardRecord
		var revision, cachedRevision *int
		var active *bool
		var wireBytes *int64
		if err := scanner.Scan(&h.HazardID, &h.Source, &h.HazardType, &h.Severity, &h.AreaName, &h.OccurredAt, &h.IngestedAt,
			&revision, &active, &cachedRevision, &wireBytes); err != nil {
			_ = scanner.Err()
			return nil, err
		}
		h.OccurredAt, h.IngestedAt = h.OccurredAt.UTC(), h.IngestedAt.UTC()
		h.Revision = 1
		if revision != nil {
			h.Revision = max(1, *revision)
		}
		if cursor != nil && h.OccurredAt.Equal(cursor.OccurredAt) && h.HazardID <= cursor.HazardID {
			continue
		}
		if !storage.MatchesHazardQuery(&h, q) {
			continue
		}
		n := 0
		valid := revision != nil && cachedRevision != nil && *revision == *cachedRevision && active != nil
		if valid && !*active {
			continue
		}
		if valid && wireBytes != nil && *wireBytes > 0 {
			n = int(*wireBytes)
		}
		if !valid || (!q.SummaryOnly && n == 0) {
			header, bytes, err := s.resolveListMetadata(ctx, &h, bucket, revision, q)
			if err != nil {
				_ = scanner.Err()
				return out, err
			}
			if header == nil {
				continue
			}
			h = *header
			n = bytes
		}
		remember(&h, n)
		out = append(out, &h)
	}
	if err := scanner.Err(); err != nil {
		return out, fmt.Errorf("failed reading hazard bucket: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	return out, nil
}
