package m2

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

var errWorkingState = errors.New("satellite_working_state_unavailable")
var errFireInput = errors.New("invalid_satellite_input")

type fireInputError struct{ cause error }

func (e fireInputError) Error() string       { return e.cause.Error() }
func (e fireInputError) Unwrap() error       { return e.cause }
func (e fireInputError) Is(other error) bool { return other == errFireInput }
func workingError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", errWorkingState, err)
}

// All graph-sized structures are indexed on disk. Only scalar policy/counters
// and the current required canonical output record live on the Go heap.
type diskFireProcessor struct {
	graph                  *workingGraph
	meta                   FireEngine
	active, fires, singles int
	keepFinal              bool
	lastSequence           int64
}

func newDiskFireProcessor(ctx context.Context, root string, lateness, retention time.Duration, maxPoints int) (*diskFireProcessor, error) {
	g, err := openWorkingGraph(ctx, root)
	if err != nil {
		return nil, workingError(err)
	}
	retention = max(retention, lateness+linkWindow)
	return &diskFireProcessor{graph: g, meta: FireEngine{Lateness: lateness, Retention: retention, MaxDetections: maxPoints}}, nil
}

func (d *diskFireProcessor) Close() error       { return d.graph.Close() }
func (d *diskFireProcessor) Active() int        { return d.active }
func (d *diskFireProcessor) Counts() (int, int) { return d.fires, d.singles }

func (d *diskFireProcessor) VisitRecords(ctx context.Context, emit func(*domain.HazardRecord) error) error {
	rows, err := d.graph.db.QueryContext(ctx, `SELECT id,payload FROM latest
		UNION ALL SELECT id,payload FROM final f WHERE NOT EXISTS(SELECT 1 FROM latest l WHERE l.id=f.id) ORDER BY id`)
	if err != nil {
		return workingError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var payload []byte
		if err := rows.Scan(&id, &payload); err != nil {
			return workingError(err)
		}
		var record domain.HazardRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return workingError(err)
		}
		if !domain.Withdrawn(&record) {
			if err := emit(&record); err != nil {
				return err
			}
		}
	}
	return workingError(rows.Err())
}

func graphPoint(ctx context.Context, tx *sql.Tx, key string) (Detection, bool, error) {
	var encoded []byte
	err := tx.QueryRowContext(ctx, `SELECT detection FROM points WHERE key=?`, key).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return Detection{}, false, nil
	}
	var point Detection
	if err == nil {
		err = json.Unmarshal(encoded, &point)
	}
	return point, err == nil, workingError(err)
}

func graphSeen(ctx context.Context, tx *sql.Tx, key string) (time.Time, bool, error) {
	var at int64
	err := tx.QueryRowContext(ctx, `SELECT at_ms FROM seen WHERE key=?`, key).Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	return time.UnixMilli(at).UTC(), err == nil, workingError(err)
}

func execGraph(ctx context.Context, tx *sql.Tx, statement string, args ...any) error {
	_, err := tx.ExecContext(ctx, statement, args...)
	return err
}

func touchGraph(ctx context.Context, tx *sql.Tx, key string) error {
	return execGraph(ctx, tx, `INSERT OR IGNORE INTO affected SELECT key FROM owners
		WHERE component_id=(SELECT component_id FROM owners WHERE key=?)`, key)
}

func graphNeighboursAffected(ctx context.Context, tx *sql.Tx, in fireInput) error {
	if in.point.Confidence == "l" {
		return nil
	}
	c := position(in.point)
	return execGraph(ctx, tx, `INSERT OR IGNORE INTO affected SELECT key FROM owners WHERE component_id IN (
		SELECT o.component_id FROM points p INDEXED BY points_grid JOIN owners o ON o.key=p.key
		WHERE p.confidence<>'l' AND p.key<>? AND p.key<>?
		AND p.cx BETWEEN ? AND ? AND p.cy BETWEEN ? AND ? AND p.cz BETWEEN ? AND ? AND p.slot BETWEEN ? AND ?
		AND ABS(p.at_ms-?)<=21600000 AND bnpb_fire_distance(p.latitude,p.longitude,?,?)<=1)`,
		in.key, in.point.Key, c[0]-1, c[0]+1, c[1]-1, c[1]+1, c[2]-1, c[2]+1, c[3]-1, c[3]+1,
		in.point.Time.UnixMilli(), in.point.Latitude, in.point.Longitude)
}

func (d *diskFireProcessor) removePoint(ctx context.Context, tx *sql.Tx, key string) error {
	var confidence string
	err := tx.QueryRowContext(ctx, `SELECT confidence FROM points WHERE key=?`, key).Scan(&confidence)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := execGraph(ctx, tx, `DELETE FROM points WHERE key=?`, key); err != nil {
		return err
	}
	d.active--
	if confidence == "l" {
		d.meta.Low--
	}
	return nil
}

// ApplyTo streams changed records to the canonical saver rather than keeping
// all outputs in memory. A local/canonical error rolls back the derived cache;
// the caller must end the term and replay the durable journal before retrying.
func (d *diskFireProcessor) ApplyTo(ctx context.Context, kind string, raw []byte, sequence int64, received time.Time, emit func(*domain.HazardRecord) error) (err error) {
	if ctx.Err() != nil {
		return workingError(ctx.Err())
	}
	if sequence != d.lastSequence+1 {
		return workingError(errors.New("noncontiguous satellite processor sequence"))
	}
	if kind == "quarantined" {
		d.lastSequence = sequence
		return nil
	}
	before, active, fires, singles := d.meta, d.active, d.fires, d.singles
	defer func() {
		if err != nil && !errors.Is(err, errFireInput) {
			d.meta, d.active, d.fires, d.singles = before, active, fires, singles
		}
	}()
	tx, err := d.graph.db.BeginTx(ctx, nil)
	if err != nil {
		return workingError(err)
	}
	defer tx.Rollback()
	in, err := prepareFireInput(&d.meta, kind, raw, d.active,
		func(key string) (Detection, bool, error) { return graphPoint(ctx, tx, key) },
		func(key string) (time.Time, bool, error) { return graphSeen(ctx, tx, key) })
	if err != nil {
		if errors.Is(err, errWorkingState) {
			return err
		}
		d.lastSequence = sequence
		return fireInputError{err}
	}
	if in.remember {
		if err := execGraph(ctx, tx, `INSERT INTO seen VALUES (?,?) ON CONFLICT(key) DO UPDATE SET at_ms=MAX(at_ms,excluded.at_ms)`,
			in.key, in.point.Time.UnixMilli()); err != nil {
			return workingError(err)
		}
	}
	if !in.skip {
		if err := d.mutate(ctx, tx, kind, in, sequence, received); err != nil {
			return workingError(err)
		}
		if emit != nil {
			if err := walkGraphKeys(ctx, tx, `SELECT id FROM changed WHERE id>? ORDER BY id LIMIT 1`, func(id string) error {
				var body []byte
				if err := tx.QueryRowContext(ctx, `SELECT payload FROM changed WHERE id=?`, id).Scan(&body); err != nil {
					return workingError(err)
				}
				var record domain.HazardRecord
				if err := json.Unmarshal(body, &record); err != nil {
					return workingError(err)
				}
				return emit(&record)
			}); err != nil {
				return err
			}
		}
	}
	if in.advance {
		if err := d.compact(ctx, tx); err != nil {
			return workingError(err)
		}
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(members>=2),0),COALESCE(SUM(members=1),0) FROM components`).Scan(&d.fires, &d.singles); err != nil {
		return workingError(err)
	}
	if err := tx.Commit(); err != nil {
		return workingError(err)
	}
	d.lastSequence = sequence
	return nil
}

func walkGraphKeys(ctx context.Context, tx *sql.Tx, query string, visit func(string) error) error {
	after := ""
	for {
		var key string
		err := tx.QueryRowContext(ctx, query, after).Scan(&key)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if key <= after {
			return errors.New("working graph cursor did not advance")
		}
		if err := visit(key); err != nil {
			return err
		}
		after = key
	}
}

func (d *diskFireProcessor) mutate(ctx context.Context, tx *sql.Tx, kind string, in fireInput, sequence int64, received time.Time) error {
	for _, table := range []string{"affected", "old_owners", "old_ids", "visited", "new_components", "component", "changed"} {
		if err := execGraph(ctx, tx, `DELETE FROM `+table); err != nil {
			return err
		}
	}
	if err := touchGraph(ctx, tx, in.key); err != nil {
		return err
	}
	if kind != "retract" {
		if err := touchGraph(ctx, tx, in.point.Key); err != nil {
			return err
		}
		if err := graphNeighboursAffected(ctx, tx, in); err != nil {
			return err
		}
	}
	if err := execGraph(ctx, tx, `INSERT INTO old_owners SELECT o.key,o.component_id FROM owners o JOIN affected a ON a.key=o.key`); err != nil {
		return err
	}
	if err := d.removePoint(ctx, tx, in.key); err != nil {
		return err
	}
	if kind != "retract" {
		if in.point.Key != in.key {
			if err := d.removePoint(ctx, tx, in.point.Key); err != nil {
				return err
			}
		}
		if err := putGraphPoint(ctx, tx, in.point); err != nil {
			return err
		}
		d.active++
		if in.point.Confidence == "l" {
			d.meta.Low++
		} else if err := execGraph(ctx, tx, `INSERT OR IGNORE INTO affected VALUES (?)`, in.point.Key); err != nil {
			return err
		}
		if err := execGraph(ctx, tx, `INSERT OR IGNORE INTO old_owners SELECT ?,component_id FROM old_owners WHERE key=?`, in.point.Key, in.key); err != nil {
			return err
		}
	}
	for _, query := range []string{
		`INSERT INTO old_ids SELECT DISTINCT component_id FROM old_owners`,
		`DELETE FROM components WHERE id IN (SELECT id FROM old_ids)`,
		`DELETE FROM owners WHERE key IN (SELECT key FROM affected)`,
	} {
		if err := execGraph(ctx, tx, query); err != nil {
			return err
		}
	}
	for {
		var root string
		err := tx.QueryRowContext(ctx, `SELECT p.key FROM affected a JOIN points p ON p.key=a.key
			LEFT JOIN visited v ON v.key=p.key WHERE v.key IS NULL AND p.confidence<>'l' ORDER BY p.key LIMIT 1`).Scan(&root)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		if in.insertOnly {
			if err := execGraph(ctx, tx, `INSERT INTO component SELECT key FROM affected`); err != nil {
				return err
			}
		} else if err := buildGraphComponent(ctx, tx, root); err != nil {
			return err
		}
		var outside int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM component c LEFT JOIN affected a ON a.key=c.key WHERE a.key IS NULL`).Scan(&outside); err != nil {
			return err
		}
		if outside != 0 {
			return errors.New("connected component escaped the affected input set")
		}
		if err := assignGraphComponent(ctx, tx, root, sequence); err != nil {
			return err
		}
		if in.insertOnly {
			break
		}
	}
	if err := walkGraphKeys(ctx, tx, `SELECT id FROM new_components WHERE id>? ORDER BY id LIMIT 1`, func(id string) error {
		var members int
		if err := tx.QueryRowContext(ctx, `SELECT members FROM components WHERE id=?`, id).Scan(&members); err != nil {
			return err
		}
		if members < 2 {
			return nil
		}
		record, err := graphRecord(ctx, tx, id, received)
		if err != nil {
			return err
		}
		previous, err := latestGraphRecord(ctx, tx, id)
		if err != nil {
			return err
		}
		if previous != nil {
			record.IngestedAt = previous.IngestedAt
		}
		if previous == nil || !sameFire(previous, record) {
			record.Attributes["stream_sequence"] = strconv.FormatInt(sequence, 10)
			if err := changeGraphRecord(ctx, tx, id, record); err != nil {
				return err
			}
		}
		return execGraph(ctx, tx, `INSERT OR IGNORE INTO published VALUES (?)`, id)
	}); err != nil {
		return err
	}
	return walkGraphKeys(ctx, tx, `SELECT o.id FROM old_ids o LEFT JOIN components c ON c.id=o.id
		WHERE o.id>? AND COALESCE(c.members,0)<2 ORDER BY o.id LIMIT 1`, func(id string) error {
		previous, err := latestGraphRecord(ctx, tx, id)
		if err != nil || previous == nil || domain.Withdrawn(previous) {
			return err
		}
		var members int
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT members FROM components WHERE id=?),0)`, id).Scan(&members); err != nil {
			return err
		}
		previous.Attributes = map[string]any{"withdrawn": true, "detection_count": members, "stream_sequence": strconv.FormatInt(sequence, 10)}
		previous.Severity = domain.SeverityNormal
		return changeGraphRecord(ctx, tx, id, previous)
	})
}

func assignGraphComponent(ctx context.Context, tx *sql.Tx, root string, sequence int64) error {
	var id string
	err := tx.QueryRowContext(ctx, `SELECT DISTINCT o.component_id FROM old_owners o JOIN component c ON c.key=o.key
		LEFT JOIN new_components n ON n.id=o.component_id WHERE n.id IS NULL
		ORDER BY EXISTS(SELECT 1 FROM published p WHERE p.id=o.component_id) DESC,o.component_id LIMIT 1`).Scan(&id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if id == "" {
		id = stableID("fire-", root)
	}
	var occupied bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM components WHERE id=?)`, id).Scan(&occupied); err != nil {
		return err
	}
	if occupied {
		id = stableID("fire-", root+"|split|"+strconv.FormatInt(sequence, 10))
	}
	if err := execGraph(ctx, tx, `INSERT INTO components SELECT ?,COUNT(*) FROM component`, id); err != nil {
		return err
	}
	if err := execGraph(ctx, tx, `INSERT INTO owners SELECT key,? FROM component`, id); err != nil {
		return err
	}
	if err := execGraph(ctx, tx, `INSERT INTO new_components VALUES (?)`, id); err != nil {
		return err
	}
	return execGraph(ctx, tx, `INSERT INTO visited SELECT key FROM component`)
}

func latestGraphRecord(ctx context.Context, tx *sql.Tx, id string) (*domain.HazardRecord, error) {
	var body []byte
	if err := tx.QueryRowContext(ctx, `SELECT payload FROM latest WHERE id=?`, id).Scan(&body); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var record domain.HazardRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func changeGraphRecord(ctx context.Context, tx *sql.Tx, id string, record *domain.HazardRecord) error {
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := execGraph(ctx, tx, `INSERT INTO latest VALUES (?,?,?,?) ON CONFLICT(id) DO UPDATE
		SET payload=excluded.payload,occurred_ms=excluded.occurred_ms,withdrawn=excluded.withdrawn`,
		id, body, record.OccurredAt.UnixMilli(), domain.Withdrawn(record)); err != nil {
		return err
	}
	return execGraph(ctx, tx, `INSERT INTO changed VALUES (?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`, id, body)
}

func graphRecord(ctx context.Context, tx *sql.Tx, id string, received time.Time) (*domain.HazardRecord, error) {
	rows, err := tx.QueryContext(ctx, `SELECT p.key,p.latitude,p.longitude,p.frp,p.satellite,p.confidence,p.at_ms
		FROM owners o JOIN points p ON p.key=o.key WHERE o.component_id=? ORDER BY p.key`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := []string{}
	satellites := keyset{}
	var x, y, z, frp float64
	var earliest time.Time
	confidence := "n"
	for rows.Next() {
		var key, satellite, quality string
		var latitude, longitude, heat float64
		var at int64
		if err := rows.Scan(&key, &latitude, &longitude, &heat, &satellite, &quality, &at); err != nil {
			return nil, err
		}
		keys = append(keys, key)
		lat, lon := latitude*math.Pi/180, longitude*math.Pi/180
		x += math.Cos(lat) * math.Cos(lon)
		y += math.Cos(lat) * math.Sin(lon)
		z += math.Sin(lat)
		frp += heat
		satellites[satellite] = true
		pointTime := time.UnixMilli(at).UTC()
		if len(keys) == 1 || pointTime.Before(earliest) {
			earliest = pointTime
		}
		if quality == "h" {
			confidence = "h"
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	lat, lon := math.Atan2(z, math.Hypot(x, y))*180/math.Pi, math.Atan2(y, x)*180/math.Pi
	severity := domain.SeverityWaspada
	if len(keys) >= 20 || frp >= 100 {
		severity = domain.SeverityAwas
	} else if len(keys) >= 6 || frp >= 30 {
		severity = domain.SeveritySiaga
	}
	return &domain.HazardRecord{Source: domain.SourceSatellite, SourceRefID: id,
		HazardType: domain.HazardTypeWildfire, Severity: severity, AreaName: nearestProvince(lat, lon),
		Latitude: lat, Longitude: lon, OccurredAt: earliest, IngestedAt: received,
		Attributes: map[string]any{"detection_count": len(keys), "satellites": sortedKeys(satellites),
			"max_confidence": confidence, "total_frp_mw": frp, "detection_keys": keys, "withdrawn": false}}, nil
}

func (d *diskFireProcessor) compact(ctx context.Context, tx *sql.Tx) error {
	cutoff := d.meta.MaxTime.Add(-d.meta.Retention).Truncate(compactStep)
	if limit := d.meta.MaxTime.Add(-d.meta.Lateness - linkWindow); cutoff.After(limit) {
		cutoff = limit
	}
	if !cutoff.After(d.meta.compacted) {
		return nil
	}
	d.meta.compacted = cutoff
	if err := execGraph(ctx, tx, `DELETE FROM expired_components`); err != nil {
		return err
	}
	if err := execGraph(ctx, tx, `INSERT INTO expired_components SELECT o.component_id
		FROM owners o JOIN points p ON p.key=o.key GROUP BY o.component_id HAVING MAX(p.at_ms)<?`, cutoff.UnixMilli()); err != nil {
		return err
	}
	if d.keepFinal {
		if err := execGraph(ctx, tx, `INSERT INTO final SELECT id,payload FROM latest WHERE id IN (SELECT id FROM expired_components)
			ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM points
		WHERE key IN (SELECT key FROM owners WHERE component_id IN (SELECT id FROM expired_components))
		OR (confidence='l' AND at_ms<?)`, cutoff.UnixMilli())
	if err != nil {
		return err
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	d.active -= int(removed)
	for _, step := range []struct {
		query string
		args  []any
	}{
		{`DELETE FROM owners WHERE component_id IN (SELECT id FROM expired_components)`, nil},
		{`DELETE FROM components WHERE id IN (SELECT id FROM expired_components)`, nil},
		{`DELETE FROM latest WHERE id IN (SELECT id FROM expired_components)`, nil},
		{`DELETE FROM published WHERE id IN (SELECT id FROM expired_components)`, nil},
		{`DELETE FROM published WHERE id IN (SELECT id FROM latest WHERE withdrawn=1 AND occurred_ms<? AND id NOT IN (SELECT id FROM components))`, []any{cutoff.UnixMilli()}},
		{`DELETE FROM latest WHERE withdrawn=1 AND occurred_ms<? AND id NOT IN (SELECT id FROM components)`, []any{cutoff.UnixMilli()}},
		{`DELETE FROM seen WHERE at_ms<?`, []any{cutoff.UnixMilli()}},
	} {
		if err := execGraph(ctx, tx, step.query, step.args...); err != nil {
			return err
		}
	}
	return nil
}
