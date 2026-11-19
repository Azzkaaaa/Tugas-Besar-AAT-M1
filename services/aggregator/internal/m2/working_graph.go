package m2

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"modernc.org/sqlite"
)

// The local graph is derived working state. The replicated input journal and
// committed checkpoint remain the authority after any process or disk loss.
// Persistent indexes and SQLite's on-disk recursive UNION replace heap-sized
// point maps, visited sets and traversal queues in the disk processor.
type workingGraph struct {
	db        *sql.DB
	directory string
}

var registerGraphDistance sync.Once
var graphDistanceError error

func openWorkingGraph(ctx context.Context, root string) (*workingGraph, error) {
	registerGraphDistance.Do(func() {
		graphDistanceError = sqlite.RegisterDeterministicScalarFunction("bnpb_fire_distance", 4,
			func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
				var values [4]float64
				for i, value := range args {
					switch v := value.(type) {
					case float64:
						values[i] = v
					case int64:
						values[i] = float64(v)
					default:
						return nil, errors.New("invalid stored graph coordinate")
					}
				}
				return distance(values[0], values[1], values[2], values[3]), nil
			})
	})
	if graphDistanceError != nil {
		return nil, graphDistanceError
	}
	if root != "" {
		if err := os.MkdirAll(root, 0700); err != nil {
			return nil, err
		}
	}
	directory, err := os.MkdirTemp(root, "satellite-graph-")
	if err != nil {
		return nil, err
	}
	g := &workingGraph{directory: directory}
	db, err := sql.Open("sqlite", filepath.Join(directory, "state.db"))
	if err != nil {
		_ = g.Close()
		return nil, err
	}
	g.db = db
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		`PRAGMA journal_mode=DELETE`, `PRAGMA synchronous=OFF`,
		`PRAGMA cache_size=-8192`, `PRAGMA temp.cache_size=-4096`,
		`PRAGMA mmap_size=0`, `PRAGMA temp_store=FILE`, `PRAGMA cache_spill=ON`,
		`PRAGMA threads=1`, `PRAGMA busy_timeout=1000`,
		`CREATE TABLE points (key TEXT PRIMARY KEY, detection BLOB NOT NULL, at_ms INTEGER NOT NULL,
			latitude REAL NOT NULL, longitude REAL NOT NULL, cx INTEGER NOT NULL, cy INTEGER NOT NULL,
			cz INTEGER NOT NULL, slot INTEGER NOT NULL, confidence TEXT NOT NULL,
			satellite TEXT NOT NULL,frp REAL NOT NULL)`,
		`CREATE INDEX points_grid ON points (cx,cy,cz,slot,key) WHERE confidence<>'l'`,
		`CREATE TABLE cells (cx INTEGER,cy INTEGER,cz INTEGER,slot INTEGER,PRIMARY KEY(cx,cy,cz,slot)) WITHOUT ROWID`,
		`CREATE TRIGGER graph_cell_insert AFTER INSERT ON points WHEN new.confidence<>'l' BEGIN
			INSERT OR IGNORE INTO cells VALUES(new.cx,new.cy,new.cz,new.slot); END`,
		`CREATE TRIGGER graph_cell_delete AFTER DELETE ON points WHEN old.confidence<>'l' BEGIN
			DELETE FROM cells WHERE cx=old.cx AND cy=old.cy AND cz=old.cz AND slot=old.slot
			AND NOT EXISTS(SELECT 1 FROM points WHERE confidence<>'l' AND cx=old.cx AND cy=old.cy AND cz=old.cz AND slot=old.slot); END`,
		`CREATE TRIGGER graph_cell_update AFTER UPDATE ON points BEGIN
			DELETE FROM cells WHERE cx=old.cx AND cy=old.cy AND cz=old.cz AND slot=old.slot
			AND NOT EXISTS(SELECT 1 FROM points WHERE confidence<>'l' AND cx=old.cx AND cy=old.cy AND cz=old.cz AND slot=old.slot);
			INSERT OR IGNORE INTO cells SELECT new.cx,new.cy,new.cz,new.slot WHERE new.confidence<>'l'; END`,
		`CREATE TABLE seen (key TEXT PRIMARY KEY,at_ms INTEGER NOT NULL)`,
		`CREATE TABLE owners (key TEXT PRIMARY KEY,component_id TEXT NOT NULL)`,
		`CREATE INDEX owners_component ON owners (component_id,key)`,
		`CREATE TABLE components (id TEXT PRIMARY KEY,members INTEGER NOT NULL)`,
		`CREATE TABLE published (id TEXT PRIMARY KEY)`,
		`CREATE TABLE latest (id TEXT PRIMARY KEY,payload BLOB NOT NULL,occurred_ms INTEGER NOT NULL,withdrawn INTEGER NOT NULL)`,
		`CREATE TABLE final (id TEXT PRIMARY KEY,payload BLOB NOT NULL)`,
		`CREATE TEMP TABLE affected (key TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE old_owners (key TEXT PRIMARY KEY,component_id TEXT NOT NULL)`,
		`CREATE TEMP TABLE visited (key TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE new_components (id TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE component (key TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE old_ids (id TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE changed (id TEXT PRIMARY KEY,payload BLOB NOT NULL)`,
		`CREATE TEMP TABLE expired_components (id TEXT PRIMARY KEY)`,
	} {
		if _, err = db.ExecContext(ctx, statement); err != nil {
			_ = g.Close()
			return nil, fmt.Errorf("initialize satellite working graph: %w", err)
		}
	}
	return g, nil
}

func (g *workingGraph) Close() error {
	var err error
	if g.db != nil {
		err = g.db.Close()
	}
	// Only remove the files of this uniquely created private graph directory.
	// Unexpected files are left intact rather than recursively deleting a path.
	for _, name := range []string{"state.db", "state.db-journal", "state.db-wal", "state.db-shm"} {
		if removeErr := os.Remove(filepath.Join(g.directory, name)); removeErr != nil && !os.IsNotExist(removeErr) {
			err = errors.Join(err, removeErr)
		}
	}
	if removeErr := os.Remove(g.directory); removeErr != nil && !os.IsNotExist(removeErr) {
		err = errors.Join(err, removeErr)
	}
	return err
}

// Fine cells are guaranteed cliques: a 250m ECEF cube has a geodesic diameter
// below 434m, and a three-hour bucket is shorter than the six-hour edge window.
// Traversal can visit occupied cells once instead of every dense point pair.
// Edges BETWEEN cells still use exact geodesic distance and event time.
func graphPosition(point Detection) cell {
	lat, lon := point.Latitude*math.Pi/180, point.Longitude*math.Pi/180
	slot := point.Time.UnixMilli() / 10800000
	if point.Time.UnixMilli() < 0 && point.Time.UnixMilli()%10800000 != 0 {
		slot--
	}
	return cell{int64(math.Floor(4 * earthKM * math.Cos(lat) * math.Cos(lon))),
		int64(math.Floor(4 * earthKM * math.Cos(lat) * math.Sin(lon))), int64(math.Floor(4 * earthKM * math.Sin(lat))), slot}
}

func putGraphPoint(ctx context.Context, tx *sql.Tx, d Detection) error {
	encoded, err := json.Marshal(d)
	if err != nil {
		return err
	}
	c := graphPosition(d)
	_, err = tx.ExecContext(ctx, `INSERT INTO points
		(key,detection,at_ms,latitude,longitude,cx,cy,cz,slot,confidence,satellite,frp) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(key) DO UPDATE SET detection=excluded.detection,at_ms=excluded.at_ms,
		latitude=excluded.latitude,longitude=excluded.longitude,cx=excluded.cx,cy=excluded.cy,
		cz=excluded.cz,slot=excluded.slot,confidence=excluded.confidence,satellite=excluded.satellite,frp=excluded.frp`,
		d.Key, encoded, d.Time.UnixMilli(), d.Latitude, d.Longitude, c[0], c[1], c[2], c[3], d.Confidence, d.Satellite, d.FRP)
	return err
}

// buildGraphComponent computes exact connected membership without retaining an
// adjacency matrix/list or a Go traversal queue. UNION deduplicates occupied
// clique cells in SQLite's disk-backed temporary B-tree. The same Haversine
// function as the reference engine decides every edge between distinct cells.
func buildGraphComponent(ctx context.Context, tx *sql.Tx, root string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM component`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `WITH RECURSIVE connected(cx,cy,cz,slot) AS (
		SELECT cx,cy,cz,slot FROM points WHERE key=? AND confidence<>'l'
		UNION
		SELECT b.cx,b.cy,b.cz,b.slot FROM connected a JOIN cells b
			ON b.cx BETWEEN a.cx-5 AND a.cx+5 AND b.cy BETWEEN a.cy-5 AND a.cy+5
			AND b.cz BETWEEN a.cz-5 AND a.cz+5 AND b.slot BETWEEN a.slot-3 AND a.slot+3
		WHERE (a.cx<>b.cx OR a.cy<>b.cy OR a.cz<>b.cz OR a.slot<>b.slot) AND EXISTS(
			SELECT 1 FROM points p INDEXED BY points_grid CROSS JOIN points q INDEXED BY points_grid
			WHERE p.confidence<>'l' AND q.confidence<>'l'
			AND p.cx=a.cx AND p.cy=a.cy AND p.cz=a.cz AND p.slot=a.slot
			AND q.cx=b.cx AND q.cy=b.cy AND q.cz=b.cz AND q.slot=b.slot
			AND ABS(p.at_ms-q.at_ms)<=21600000
			AND bnpb_fire_distance(p.latitude,p.longitude,q.latitude,q.longitude)<=1)
	) INSERT INTO component SELECT p.key FROM connected c JOIN points p INDEXED BY points_grid
		ON p.cx=c.cx AND p.cy=c.cy AND p.cz=c.cz AND p.slot=c.slot WHERE p.confidence<>'l'`, root)
	return err
}
