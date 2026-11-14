package cassandra

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync/atomic"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// The bucket period can change while the store is in use. The schedule is a list of segments in
// bucket_schedule: from each segment's start on, hazards are bucketed with its period. A
// hazard's bucket depends only on its occurred_at, so it is the same for every writer and
// reader that has the same schedule.
//
// Segments are only ever appended, starting at a boundary that is still in the future (at least
// scheduleChangeLead away) and that both the old and the new period divide. So no existing bucket
// changes, the last bucket of the old period ends exactly where the first new one starts, and
// every process re-reading the schedule (every scheduleRefresh) knows the new segment before
// it starts. Rows also record the bucket they were written to (hazards_by_id.bucket).

const (
	scheduleScope       = "hazards"
	scheduleRefresh     = 30 * time.Second
	scheduleChangeLead  = 2 * scheduleRefresh
	maxBucketPeriod     = 24 * time.Hour
	scheduleOriginLabel = "1970-01-01T00:00:00Z"
)

// scheduleOrigin is the start of the first segment: before any hazard.
var scheduleOrigin = time.Unix(0, 0).UTC()

type segment struct {
	From   time.Time
	Period time.Duration
}

// schedule is sorted by From; the first segment starts at scheduleOrigin.
type schedule []segment

// segmentAt returns the segment covering t (the first one for times before it).
func (s schedule) segmentAt(t time.Time) segment {
	i := sort.Search(len(s), func(i int) bool { return s[i].From.After(t) })
	if i == 0 {
		return s[0]
	}
	return s[i-1]
}

// bucketOf returns the start of the bucket holding t.
func (s schedule) bucketOf(t time.Time) time.Time {
	return t.UTC().Truncate(s.segmentAt(t).Period)
}

// current returns the segment in effect at now and the next scheduled one, if any.
func (s schedule) current(now time.Time) (segment, *segment) {
	cur := s.segmentAt(now)
	for i := range s {
		if s[i].From.After(now) {
			return cur, &s[i]
		}
	}
	return cur, nil
}

// ValidateBucketPeriod accepts whole seconds from 1s to 24h that divide 24h, so bucket
// boundaries fall on the same wall-clock times every day.
func ValidateBucketPeriod(p time.Duration) error {
	if p < time.Second || p > maxBucketPeriod || p%time.Second != 0 || maxBucketPeriod%p != 0 {
		return fmt.Errorf("bucket period %s must be whole seconds between 1s and 24h and divide 24h evenly (e.g. 1m, 5m, 1h, 24h)", p)
	}
	return nil
}

// changeBoundary is the first time at or after earliest that both periods divide. Both divide
// 24h, so their least common multiple does too, and the boundary is at most 24h after earliest.
func changeBoundary(earliest time.Time, a, b time.Duration) time.Time {
	l := lcm(a, b)
	t := earliest.UTC().Truncate(l)
	if t.Before(earliest) {
		t = t.Add(l)
	}
	return t
}

func lcm(a, b time.Duration) time.Duration {
	x, y := int64(a/time.Second), int64(b/time.Second)
	g, h := x, y
	for h != 0 {
		g, h = h, g%h
	}
	return time.Duration(x/g*y) * time.Second
}

// scheduleHolder is the schedule a DB uses, swapped atomically on refresh.
type scheduleHolder struct{ p atomic.Pointer[schedule] }

func (h *scheduleHolder) get() schedule       { return *h.p.Load() }
func (h *scheduleHolder) set(s schedule)      { h.p.Store(&s) }
func (db *DB) schedule() schedule             { return db.sched.get() }
func (db *DB) bucketOf(t time.Time) time.Time { return db.schedule().bucketOf(t) }

// loadSchedule reads bucket_schedule.
func (db *DB) loadSchedule(ctx context.Context) (schedule, error) {
	iter := db.session.Query(`SELECT effective_from, period_seconds FROM bucket_schedule WHERE scope = ?`, scheduleScope).
		WithContext(ctx).Iter()
	var s schedule
	var from time.Time
	var secs int
	for iter.Scan(&from, &secs) {
		s = append(s, segment{From: from.UTC(), Period: time.Duration(secs) * time.Second})
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	sort.Slice(s, func(i, j int) bool { return s[i].From.Before(s[j].From) })
	return s, nil
}

// initSchedule loads the schedule, creating its first segment when the keyspace has none: the
// period recorded by older builds in store_meta, or else initial.
func (db *DB) initSchedule(ctx context.Context, initial time.Duration) error {
	s, err := db.loadSchedule(ctx)
	if err != nil {
		return err
	}
	if len(s) == 0 {
		period := initial
		var legacy string
		err := db.session.Query(`SELECT value FROM store_meta WHERE key = ?`, metaBucketPeriod).WithContext(ctx).Scan(&legacy)
		if err == nil {
			if p, perr := time.ParseDuration(legacy); perr == nil && ValidateBucketPeriod(p) == nil {
				period = p
			}
		} else if !errors.Is(err, gocql.ErrNotFound) {
			return err
		}
		if err := ValidateBucketPeriod(period); err != nil {
			return err
		}
		if _, err := db.session.Query(`INSERT INTO bucket_schedule (scope, effective_from, period_seconds)
			VALUES (?, ?, ?) IF NOT EXISTS`, scheduleScope, scheduleOrigin, int(period/time.Second)).
			WithContext(ctx).MapScanCAS(map[string]any{}); err != nil {
			return err
		}
		if s, err = db.loadSchedule(ctx); err != nil {
			return err
		}
	}
	if len(s) == 0 || !s[0].From.Equal(scheduleOrigin) {
		return fmt.Errorf("bucket_schedule has no segment starting at %s", scheduleOriginLabel)
	}
	db.sched.set(s)

	if cur, next := s.current(time.Now()); initial > 0 && initial != cur.Period && (next == nil || next.Period != initial) {
		log.Printf("[Cassandra] Configured bucket period %s is not in effect (store uses %s); it only applies to a new keyspace. Change the period with storeadmin.",
			initial, cur.Period)
	}
	return nil
}

// refreshSchedule re-reads the schedule every scheduleRefresh until ctx is done, so a change made
// by another process (cmd/storeadmin) is known before its segment starts.
func (db *DB) refreshSchedule(ctx context.Context) {
	ticker := time.NewTicker(scheduleRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s, err := db.loadSchedule(ctx)
			if err != nil || len(s) == 0 {
				if ctx.Err() == nil {
					log.Printf("[Cassandra] Failed refreshing bucket schedule (keeping the last one): %v", err)
				}
				continue
			}
			if len(s) != len(db.schedule()) {
				cur, _ := s.current(time.Now())
				log.Printf("[Cassandra] Bucket schedule changed: %d segments, current period %s", len(s), cur.Period)
			}
			db.sched.set(s)
		}
	}
}

// BucketPeriodStatus describes the period in effect and any scheduled change.
type BucketPeriodStatus struct {
	Current     time.Duration
	CurrentFrom time.Time
	Next        time.Duration // 0 when no change is scheduled
	NextFrom    time.Time
}

func (db *DB) BucketPeriod(ctx context.Context) (BucketPeriodStatus, error) {
	s, err := db.loadSchedule(ctx)
	if err != nil {
		return BucketPeriodStatus{}, err
	}
	if len(s) == 0 {
		return BucketPeriodStatus{}, fmt.Errorf("no bucket schedule")
	}
	cur, next := s.current(time.Now())
	st := BucketPeriodStatus{Current: cur.Period, CurrentFrom: cur.From}
	if next != nil {
		st.Next, st.NextFrom = next.Period, next.From
	}
	return st, nil
}

// SetBucketPeriod schedules period from the first boundary that both it and the current period
// divide, at least scheduleChangeLead from now, and returns when it takes effect. Setting the
// period already in effect is a no-op returning the zero time; a change already scheduled must
// take effect before another is made.
func (db *DB) SetBucketPeriod(ctx context.Context, period time.Duration) (time.Time, error) {
	if err := ValidateBucketPeriod(period); err != nil {
		return time.Time{}, err
	}
	s, err := db.loadSchedule(ctx)
	if err != nil {
		return time.Time{}, err
	}
	now := time.Now().UTC()
	cur, next := s.current(now)
	if next != nil {
		return time.Time{}, fmt.Errorf("a change to %s is already scheduled at %s; set the period again after it",
			next.Period, next.From.Format(time.RFC3339))
	}
	if period == cur.Period {
		return time.Time{}, nil
	}
	from := changeBoundary(now.Add(scheduleChangeLead), cur.Period, period)
	if err := db.addSegment(ctx, from, period); err != nil {
		return time.Time{}, err
	}
	return from, nil
}

// addSegment appends a segment. Callers guarantee from is aligned to both periods and late enough.
func (db *DB) addSegment(ctx context.Context, from time.Time, period time.Duration) error {
	applied, err := db.session.Query(`INSERT INTO bucket_schedule (scope, effective_from, period_seconds)
		VALUES (?, ?, ?) IF NOT EXISTS`, scheduleScope, from, int(period/time.Second)).
		WithContext(ctx).MapScanCAS(map[string]any{})
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("a schedule segment at %s already exists", from.Format(time.RFC3339))
	}
	s, err := db.loadSchedule(ctx)
	if err != nil {
		return err
	}
	db.sched.set(s)
	return nil
}
