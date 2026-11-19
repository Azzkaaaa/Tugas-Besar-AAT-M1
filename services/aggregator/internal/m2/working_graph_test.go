package m2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func testWorkingGraph(t *testing.T) *workingGraph {
	t.Helper()
	g, err := openWorkingGraph(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Error(err)
		}
	})
	return g
}

func TestDenseDiskConnectivityVisitsCliqueCells(t *testing.T) {
	g := testWorkingGraph(t)
	ctx := context.Background()
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	const count = 6000
	for i := 0; i < count; i++ {
		point := Detection{Key: fmt.Sprintf("dense-%05d", i), Time: testNow, Latitude: .0001 + float64(i%60)*.0000001, Longitude: .0001 + float64(i/60)*.0000001, Confidence: "n"}
		if err := putGraphPoint(ctx, tx, point); err != nil {
			t.Fatal(err)
		}
	}
	var cells int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cells`).Scan(&cells); err != nil || cells != 1 {
		t.Fatal("dense fixture must occupy one clique cell", cells, err)
	}
	bounded, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	started := time.Now()
	if err := buildGraphComponent(bounded, tx, "dense-00000"); err != nil {
		t.Fatal("dense traversal failed", err)
	}
	var got int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM component`).Scan(&got); err != nil || got != count {
		t.Fatal(got, err)
	}
	t.Logf("dense_points=%d traversal=%s", count, time.Since(started))
}

func TestDiskCellTraversalMatchesIndependentAllPairs(t *testing.T) {
	g := testWorkingGraph(t)
	ctx := context.Background()
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rng := rand.New(rand.NewSource(71))
	points := make([]Detection, 400)
	areas := [][2]float64{{0, 179.999}, {0, -179.999}, {89.98, 100}, {-6.18, 106.8}}
	for i := range points {
		area := areas[i%len(areas)]
		lon := area[1] + (rng.Float64()-.5)*.015
		if lon > 180 {
			lon -= 360
		}
		if lon < -180 {
			lon += 360
		}
		points[i] = Detection{Key: fmt.Sprintf("oracle-%04d", i), Time: time.Unix(-21600, 0).Add(time.Duration(rng.Intn(721)) * time.Minute), Latitude: area[0] + (rng.Float64()-.5)*.015, Longitude: lon, Confidence: "n"}
		if i%9 == 0 {
			points[i].Confidence = "l"
		}
		if err := putGraphPoint(ctx, tx, points[i]); err != nil {
			t.Fatal(err)
		}
	}
	visited := map[int]bool{}
	for root, p := range points {
		if visited[root] || p.Confidence == "l" {
			continue
		}
		queue := []int{root}
		visited[root] = true
		var want []string
		for len(queue) > 0 {
			i := queue[0]
			queue = queue[1:]
			a := points[i]
			want = append(want, a.Key)
			for j, b := range points {
				if visited[j] || b.Confidence == "l" {
					continue
				}
				dt := a.Time.Sub(b.Time)
				if dt < 0 {
					dt = -dt
				}
				if dt <= linkWindow && distance(a.Latitude, a.Longitude, b.Latitude, b.Longitude) <= 1 {
					visited[j] = true
					queue = append(queue, j)
				}
			}
		}
		sort.Strings(want)
		got := graphKeys(t, tx, p.Key)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("root %s differs: got %v want %v", p.Key, got, want)
		}
	}
}

func TestDiskCellIndexTracksDeleteMoveAndConfidenceChanges(t *testing.T) {
	g := testWorkingGraph(t)
	ctx := context.Background()
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	point := Detection{Key: "one", Time: testNow, Latitude: 1, Longitude: 100, Confidence: "n"}
	if err := putGraphPoint(ctx, tx, point); err != nil {
		t.Fatal(err)
	}
	point.Key = "two"
	if err := putGraphPoint(ctx, tx, point); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM points WHERE key='one'`); err != nil {
		t.Fatal(err)
	}
	point.Latitude = 2
	if err := putGraphPoint(ctx, tx, point); err != nil {
		t.Fatal(err)
	}
	var orphan int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cells c WHERE NOT EXISTS (SELECT 1 FROM points p WHERE p.cx=c.cx AND p.cy=c.cy AND p.cz=c.cz AND p.slot=c.slot AND p.confidence<>'l')`).Scan(&orphan); err != nil || orphan != 0 {
		t.Fatal("stale clique cell", orphan, err)
	}
	point.Confidence = "l"
	if err := putGraphPoint(ctx, tx, point); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cells`).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatal("low-confidence cell retained", remaining, err)
	}
}

func graphKeys(t *testing.T, tx *sql.Tx, root string) []string {
	t.Helper()
	if err := buildGraphComponent(context.Background(), tx, root); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(`SELECT key FROM component ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestWorkingGraphHasBoundedCachesAndNoMemoryMappedState(t *testing.T) {
	g := testWorkingGraph(t)
	for pragma, want := range map[string]int{"cache_size": -8192, "temp.cache_size": -4096,
		"mmap_size": 0, "temp_store": 1, "cache_spill": 1} {
		var value int
		if err := g.db.QueryRow("PRAGMA " + pragma).Scan(&value); err != nil {
			t.Fatal(pragma, err)
		}
		if pragma == "cache_spill" {
			if value == 0 {
				t.Fatal("dirty pages must be allowed to spill to disk")
			}
		} else if value != want {
			t.Fatalf("%s=%d want %d", pragma, value, want)
		}
	}
}

func TestDiskConnectivityMatchesReferenceAndHandlesBridgeRemoval(t *testing.T) {
	g := testWorkingGraph(t)
	ctx := context.Background()
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	e := NewFireEngine(24*time.Hour, 1000)
	var inserted []Detection
	for i := 0; i < 7; i++ {
		confidence := "n"
		if i == 6 {
			confidence = "l"
		}
		body := detection(fmt.Sprintf("N%d", i), "600", "0", fmt.Sprintf("%.4f", float64(i)*.008), confidence, 2)
		object, _ := decodeObject(body)
		point, err := parseDetection(object)
		if err != nil {
			t.Fatal(err)
		}
		inserted = append(inserted, point)
		if err := putGraphPoint(ctx, tx, point); err != nil {
			t.Fatal(err)
		}
		applyOK(t, e, "detection", body, int64(i+1))
	}
	for _, point := range inserted[:6] {
		want := sortedKeys(e.groups[e.owner[point.Key]])
		if got := graphKeys(t, tx, point.Key); !reflect.DeepEqual(got, want) {
			t.Fatal("disk membership differs from indexed reference", got, want)
		}
	}
	if got := graphKeys(t, tx, inserted[6].Key); len(got) != 0 {
		t.Fatal("low confidence point entered graph", got)
	}
	if _, err := tx.Exec(`DELETE FROM points WHERE key=?`, inserted[2].Key); err != nil {
		t.Fatal(err)
	}
	if got := graphKeys(t, tx, inserted[0].Key); len(got) != 2 {
		t.Fatal("removing a bridge did not split the disk graph", got)
	}
	if got := graphKeys(t, tx, inserted[3].Key); len(got) != 3 {
		t.Fatal("wrong component after bridge removal", got)
	}
}

func TestDiskConnectivityUsesGeodesicDistanceAndEventTime(t *testing.T) {
	g := testWorkingGraph(t)
	ctx := context.Background()
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for i, point := range []Detection{
		{Key: "a", Time: start, Latitude: 0, Longitude: 179.999, Confidence: "n"},
		{Key: "b", Time: start.Add(6 * time.Hour), Latitude: 0, Longitude: -179.999, Confidence: "n"},
		{Key: "c", Time: start.Add(12*time.Hour + time.Minute), Latitude: 0, Longitude: -179.999, Confidence: "n"},
	} {
		if err := putGraphPoint(ctx, tx, point); err != nil {
			t.Fatal(i, err)
		}
	}
	if got := graphKeys(t, tx, "a"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatal("dateline or six-hour boundary was evaluated incorrectly", got)
	}
}

func TestClosingWorkingGraphRemovesOnlyItsOwnDirectory(t *testing.T) {
	root := t.TempDir()
	other := root + string(os.PathSeparator) + "keep.txt"
	if err := os.WriteFile(other, []byte("unrelated"), 0600); err != nil {
		t.Fatal(err)
	}
	g, err := openWorkingGraph(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(g.directory); !os.IsNotExist(err) {
		t.Fatal("private graph directory remains", err)
	}
	if _, err := os.Stat(other); err != nil {
		t.Fatal("unrelated file was touched", err)
	}
}

func TestWorkingGraphSpillsStateBeyondItsPagerCache(t *testing.T) {
	g := testWorkingGraph(t)
	ctx := context.Background()
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	point := Detection{Time: testNow, Latitude: 1, Longitude: 100, Confidence: "n",
		Raw: map[string]json.RawMessage{"vendor": json.RawMessage(`"` + strings.Repeat("x", 4096) + `"`)}}
	for i := 0; i < 5000; i++ {
		point.Key = fmt.Sprintf("point-%05d", i)
		if err := putGraphPoint(ctx, tx, point); err != nil {
			t.Fatal(i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var pages, pageSize, count int64
	for query, value := range map[string]*int64{"PRAGMA page_count": &pages,
		"PRAGMA page_size": &pageSize, "SELECT COUNT(*) FROM points": &count} {
		if err := g.db.QueryRowContext(ctx, query).Scan(value); err != nil {
			t.Fatal(err)
		}
	}
	if count != 5000 || pages*pageSize <= 8<<20 {
		t.Fatalf("working state did not exceed and spill beyond the 8MiB cache: points=%d bytes=%d", count, pages*pageSize)
	}
}
