package m2

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
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
			cz INTEGER NOT NULL, slot INTEGER NOT NULL, confidence TEXT NOT NULL)`,
		`CREATE INDEX points_grid ON points (cx,cy,cz,slot,key) WHERE confidence<>'l'`,
		`CREATE TABLE seen (key TEXT PRIMARY KEY,at_ms INTEGER NOT NULL)`,
		`CREATE TABLE owners (key TEXT PRIMARY KEY,component_id TEXT NOT NULL)`,
		`CREATE INDEX owners_component ON owners (component_id,key)`,
		`CREATE TABLE components (id TEXT PRIMARY KEY,members INTEGER NOT NULL)`,
		`CREATE TABLE published (id TEXT PRIMARY KEY)`,
		`CREATE TABLE latest (id TEXT PRIMARY KEY,payload BLOB NOT NULL)`,
		`CREATE TABLE final (id TEXT PRIMARY KEY,payload BLOB NOT NULL)`,
		`CREATE TEMP TABLE affected (key TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE old_owners (key TEXT PRIMARY KEY,component_id TEXT NOT NULL)`,
		`CREATE TEMP TABLE visited (key TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE new_components (id TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE component (key TEXT PRIMARY KEY)`,
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

func putGraphPoint(ctx context.Context, tx *sql.Tx, d Detection) error {
	encoded, err := json.Marshal(d)
	if err != nil {
		return err
	}
	c := position(d)
	_, err = tx.ExecContext(ctx, `INSERT INTO points
		(key,detection,at_ms,latitude,longitude,cx,cy,cz,slot,confidence) VALUES (?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(key) DO UPDATE SET detection=excluded.detection,at_ms=excluded.at_ms,
		latitude=excluded.latitude,longitude=excluded.longitude,cx=excluded.cx,cy=excluded.cy,
		cz=excluded.cz,slot=excluded.slot,confidence=excluded.confidence`,
		d.Key, encoded, d.Time.UnixMilli(), d.Latitude, d.Longitude, c[0], c[1], c[2], c[3], d.Confidence)
	return err
}

// buildGraphComponent computes exact connected membership without retaining an
// adjacency matrix/list or a Go traversal queue. UNION deduplicates visited
// keys in SQLite's disk-backed temporary B-tree. The index narrows candidates,
// and the same Haversine function as the reference engine decides every edge.
func buildGraphComponent(ctx context.Context, tx *sql.Tx, root string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM component`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `WITH RECURSIVE connected(key) AS (
		SELECT key FROM points WHERE key=? AND confidence<>'l'
		UNION
		SELECT p.key FROM connected c JOIN points a ON a.key=c.key
		JOIN points p INDEXED BY points_grid ON p.cx BETWEEN a.cx-1 AND a.cx+1
			AND p.cy BETWEEN a.cy-1 AND a.cy+1 AND p.cz BETWEEN a.cz-1 AND a.cz+1
			AND p.slot BETWEEN a.slot-1 AND a.slot+1
		WHERE p.confidence<>'l' AND ABS(p.at_ms-a.at_ms)<=21600000
			AND bnpb_fire_distance(a.latitude,a.longitude,p.latitude,p.longitude)<=1
	) INSERT INTO component SELECT key FROM connected`, root)
	return err
}
