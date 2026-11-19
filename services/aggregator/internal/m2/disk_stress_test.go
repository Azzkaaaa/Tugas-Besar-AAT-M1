package m2

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

// Opt in inside a real 512MiB container. Default unit runs stay lightweight.
// This is synthetic working-state evidence, not official-feed acceptance.
func TestDiskProcessorMemoryEnvelope(t *testing.T) {
	if os.Getenv("M2_DISK_STRESS") != "1" {
		t.Skip("set M2_DISK_STRESS=1 in a memory-capped disposable process")
	}
	n := 25000
	if raw := os.Getenv("M2_DISK_POINTS"); raw != "" {
		var err error
		n, err = strconv.Atoi(raw)
		if err != nil || n < 1000 || n > 200000 {
			t.Fatal("M2_DISK_POINTS must be 1000..200000")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	root := t.TempDir()
	d, err := newDiskFireProcessor(ctx, root, 24*time.Hour, 48*time.Hour, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	started := time.Now()
	for i := 0; i < n; i++ {
		lat := fmt.Sprintf("%.6f", -30+float64(i/400)*.02)
		lon := fmt.Sprintf("%.6f", 95+float64(i%400)*.02)
		if err := d.ApplyTo(ctx, "detection", detection("N20", "600", lat, lon, "n", 10), int64(i+1), testNow, nil); err != nil {
			t.Fatal(i, err)
		}
		if (i+1)%10000 == 0 {
			t.Logf("processed=%d elapsed=%s", i+1, time.Since(started))
		}
	}
	if d.Active() != n || d.fires != 0 || d.singles != n {
		t.Fatal("independent components differ", d.Active(), d.fires, d.singles)
	}
	dbInfo, err := os.Stat(filepath.Join(d.graph.directory, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(root, "measured-snapshot-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	if err := d.WriteState(ctx, f, int64(n)); err != nil {
		t.Fatal(err)
	}
	snapshotInfo, _ := f.Stat()
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	d, err = restoreDiskFireProcessor(ctx, root, 24*time.Hour, 48*time.Hour, 1000, int64(n), f)
	if err != nil {
		t.Fatal(err)
	}
	if d.Active() != n || d.fires != 0 || d.singles != n {
		t.Fatal("restored component counts differ")
	}
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	rss, containerPeak := diskMemoryPeaks()
	t.Logf("points=%d sqlite_bytes=%d snapshot_bytes=%d heap_bytes=%d peak_rss=%s container_peak_bytes=%s elapsed=%s", n, dbInfo.Size(), snapshotInfo.Size(), memory.HeapAlloc, rss, containerPeak, time.Since(started))
}

func diskMemoryPeaks() (rss, container string) {
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				rss = strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
			}
		}
	}
	if peak, err := os.ReadFile("/sys/fs/cgroup/memory.peak"); err == nil {
		container = strings.TrimSpace(string(peak))
	}
	return
}

// Seed a valid large component directly into its derived cache to isolate the
// transient memory cost of ONE required canonical record and its correction.
// This does not claim to measure full-feed ingestion or remote storage latency.
func TestDiskDenseCorrectionMemoryEnvelope(t *testing.T) {
	if os.Getenv("M2_DISK_STRESS") != "1" {
		t.Skip("set M2_DISK_STRESS=1 in a memory-capped disposable process")
	}
	n := 25000
	if raw := os.Getenv("M2_DISK_POINTS"); raw != "" {
		var err error
		n, err = strconv.Atoi(raw)
		if err != nil || n < 1000 || n > 200000 {
			t.Fatal("M2_DISK_POINTS must be 1000..200000")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	started := time.Now()
	root := t.TempDir()
	d, err := newDiskFireProcessor(ctx, root, 24*time.Hour, 48*time.Hour, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	tx, err := d.graph.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var id, retracted string
	for i := 0; i < n; i++ {
		raw := detection("N20", "600", fmt.Sprintf("%.8f", .0001+float64(i%500)*.00000001), fmt.Sprintf("%.8f", .0001+float64(i/500)*.00000001), "n", 10)
		object, err := decodeObject(raw)
		if err != nil {
			t.Fatal(err)
		}
		point, err := parseDetection(object)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			id = stableID("fire-", point.Key)
			retracted = point.Key
			d.meta.MaxTime = point.Time
		}
		if err := putGraphPoint(ctx, tx, point); err != nil {
			t.Fatal(err)
		}
		if err := execGraph(ctx, tx, `INSERT INTO owners VALUES (?,?)`, point.Key, id); err != nil {
			t.Fatal(err)
		}
		if err := execGraph(ctx, tx, `INSERT INTO seen VALUES (?,?)`, point.Key, point.Time.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	if err := execGraph(ctx, tx, `INSERT INTO components VALUES (?,?)`, id, n); err != nil {
		t.Fatal(err)
	}
	if err := execGraph(ctx, tx, `INSERT INTO published VALUES (?)`, id); err != nil {
		t.Fatal(err)
	}
	baseline, err := graphRecord(ctx, tx, id, testNow)
	if err != nil {
		t.Fatal(err)
	}
	baseline.Attributes["stream_sequence"] = strconv.Itoa(n)
	if err := changeGraphRecord(ctx, tx, id, baseline); err != nil {
		t.Fatal(err)
	}
	baseline = nil
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	d.active, d.fires, d.lastSequence = n, 1, int64(n)
	raw, _ := json.Marshal(map[string]string{"key": retracted})
	emitted := 0
	correctionStarted := time.Now()
	if err := d.ApplyTo(ctx, "retract", raw, int64(n+1), testNow, func(record *domain.HazardRecord) error {
		emitted++
		if record.SourceRefID != id || fmt.Sprint(record.Attributes["detection_count"]) != strconv.Itoa(n-1) {
			t.Fatal("dense correction changed identity or membership")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if emitted != 1 || d.active != n-1 || d.fires != 1 || d.singles != 0 {
		t.Fatal("dense correction counters differ")
	}
	rss, containerPeak := diskMemoryPeaks()
	t.Logf("dense_points=%d emitted=%d peak_rss=%s container_peak_bytes=%s correction=%s elapsed=%s", n, emitted, rss, containerPeak, time.Since(correctionStarted), time.Since(started))
}
