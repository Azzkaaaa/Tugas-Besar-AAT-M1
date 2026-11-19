package m2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

func testDiskProcessor(t *testing.T, maxPoints int) *diskFireProcessor {
	t.Helper()
	disk, err := newDiskFireProcessor(context.Background(), t.TempDir(), 24*time.Hour, 48*time.Hour, maxPoints)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	})
	return disk
}

func compareDiskInput(t *testing.T, disk *diskFireProcessor, reference *FireEngine, input streamInput, sequence int64) {
	t.Helper()
	want, wantErr := reference.Apply(input.kind, input.payload, sequence, testNow)
	var got []*domain.HazardRecord
	err := disk.ApplyTo(context.Background(), input.kind, input.payload, sequence, testNow, func(h *domain.HazardRecord) error {
		got = append(got, h)
		return nil
	})
	if wantErr != nil {
		if err == nil || err.Error() != wantErr.Error() || !errors.Is(err, errFireInput) {
			t.Fatalf("input %d: disk=%v reference=%v", sequence, err, wantErr)
		}
	} else if err != nil {
		t.Fatalf("input %d: disk failed: %v", sequence, err)
	}
	if !reflect.DeepEqual(outputs(t, got), outputs(t, want)) {
		t.Fatalf("input %d: emitted revisions differ\n got %s\nwant %s", sequence, outputs(t, got), outputs(t, want))
	}
	fires, singles := reference.Counts()
	if disk.active != reference.Active() || disk.fires != fires || disk.singles != singles ||
		disk.meta.Late != reference.Late || disk.meta.Low != reference.Low || disk.meta.Expired != reference.Expired ||
		disk.meta.OverCapacity != reference.OverCapacity || !disk.meta.MaxTime.Equal(reference.MaxTime) ||
		!disk.meta.compacted.Equal(reference.compacted) {
		t.Fatalf("input %d: counters differ: disk active/fires/single=%d/%d/%d late/low/expired/pressure=%d/%d/%d/%d; reference %d/%d/%d %d/%d/%d/%d",
			sequence, disk.active, disk.fires, disk.singles, disk.meta.Late, disk.meta.Low, disk.meta.Expired, disk.meta.OverCapacity,
			reference.Active(), fires, singles, reference.Late, reference.Low, reference.Expired, reference.OverCapacity)
	}
}

func TestDiskProcessorMatchesEveryRevisionAcrossLongFeed(t *testing.T) {
	disk := testDiskProcessor(t, 4)
	reference := NewFireEngineRetention(24*time.Hour, 48*time.Hour, 4)
	disk.keepFinal, reference.KeepFinal = true, true
	for i, input := range longFeed(5) {
		compareDiskInput(t, disk, reference, input, int64(i+1))
	}
	var got []*domain.HazardRecord
	if err := disk.VisitRecords(context.Background(), func(h *domain.HazardRecord) error { got = append(got, h); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(outputs(t, got), outputs(t, reference.Snapshot())) {
		t.Fatal("finalized disk catalog differs from uninterrupted reference")
	}
}

func TestDiskProcessorMatchesCoordinateConfidenceUpdatesAndSplits(t *testing.T) {
	disk := testDiskProcessor(t, 4)
	reference := NewFireEngineRetention(24*time.Hour, 48*time.Hour, 4)
	random := rand.New(rand.NewSource(19))
	for sequence := 1; sequence <= 200; sequence++ {
		kind := "detection"
		body := detection(fmt.Sprintf("N%d", sequence), fmt.Sprint(600+sequence%50),
			fmt.Sprintf("%.4f", float64(random.Intn(12))*.006),
			fmt.Sprintf("%.4f", 100+float64(random.Intn(12))*.006), "n", 3)
		if sequence > 80 {
			keys := sortedKeys(reference.points)
			key := keys[random.Intn(len(keys))]
			kind = "update"
			changes := map[string]any{"latitude": float64(random.Intn(12)) * .006,
				"longitude": 100 + float64(random.Intn(12))*.006, "confidence": "n"}
			if sequence%4 == 0 {
				changes["confidence"] = "l"
			}
			if sequence%3 == 0 {
				kind = "retract"
				body, _ = json.Marshal(map[string]any{"key": key})
			} else {
				body, _ = json.Marshal(map[string]any{"key": key, "changes": changes})
			}
		}
		compareDiskInput(t, disk, reference, streamInput{kind, body}, int64(sequence))
	}
}

func TestDiskProcessorRollsBackWhenCanonicalEmissionFails(t *testing.T) {
	disk := testDiskProcessor(t, 4)
	reference := NewFireEngineRetention(24*time.Hour, 48*time.Hour, 4)
	for i := 0; i < 2; i++ {
		input := streamInput{"detection", detection(fmt.Sprintf("N%d", i), "600", "0", "0", "n", 5)}
		compareDiskInput(t, disk, reference, input, int64(i+1))
	}
	third := streamInput{"detection", detection("N2", "600", "0", "0", "h", 100)}
	saveErr := errors.New("canonical write failed")
	err := disk.ApplyTo(context.Background(), third.kind, third.payload, 3, testNow, func(*domain.HazardRecord) error { return saveErr })
	if !errors.Is(err, saveErr) || errors.Is(err, errFireInput) {
		t.Fatal("save failure was hidden or classified as invalid input", err)
	}
	compareDiskInput(t, disk, reference, third, 3)
}

func TestDiskStateErrorsAreNeverPoisonInput(t *testing.T) {
	disk := testDiskProcessor(t, 4)
	if err := disk.graph.db.Close(); err != nil {
		t.Fatal(err)
	}
	err := disk.ApplyTo(context.Background(), "detection", detection("N20", "600", "0", "0", "n", 5), 1, testNow, nil)
	if !errors.Is(err, errWorkingState) || errors.Is(err, errFireInput) || disk.active != 0 {
		t.Fatal("working-disk failure was swallowed or could advance processing", err)
	}
}
