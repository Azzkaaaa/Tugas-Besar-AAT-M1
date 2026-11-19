package m2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
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
	var rss string
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				rss = strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
			}
		}
	}
	t.Logf("points=%d sqlite_bytes=%d snapshot_bytes=%d heap_bytes=%d peak_rss=%s elapsed=%s", n, dbInfo.Size(), snapshotInfo.Size(), memory.HeapAlloc, rss, time.Since(started))
}
