package m2

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"
)

func diskStateBytes(t *testing.T, disk *diskFireProcessor) []byte {
	t.Helper()
	var state bytes.Buffer
	if err := disk.WriteState(context.Background(), &state, disk.lastSequence); err != nil {
		t.Fatal(err)
	}
	return state.Bytes()
}

func restoreTestDisk(t *testing.T, disk *diskFireProcessor, state []byte, sequence int64) *diskFireProcessor {
	t.Helper()
	restored, err := restoreDiskFireProcessor(context.Background(), t.TempDir(), disk.meta.Lateness,
		disk.meta.Retention, disk.meta.MaxDetections, sequence, bytes.NewReader(state))
	if err != nil {
		t.Fatal(err)
	}
	return restored
}

func TestDiskSnapshotsResumeIdenticalRevisionsAtMultipleCutPoints(t *testing.T) {
	for _, every := range []int{1, 37, 250} {
		t.Run(fmt.Sprint(every), func(t *testing.T) {
			disk, err := newDiskFireProcessor(context.Background(), t.TempDir(), 24*time.Hour, 48*time.Hour, 4)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := disk.Close(); err != nil {
					t.Error(err)
				}
			}()
			disk.keepFinal = true
			reference := NewFireEngineRetention(24*time.Hour, 48*time.Hour, 4)
			for i, input := range longFeed(3) {
				if i > 0 && i%every == 0 {
					state := diskStateBytes(t, disk)
					restored := restoreTestDisk(t, disk, state, int64(i))
					if again := diskStateBytes(t, restored); !bytes.Equal(state, again) {
						t.Fatal("logical snapshot bytes changed after restoration")
					}
					if err := disk.Close(); err != nil {
						t.Fatal(err)
					}
					disk = restored
				}
				compareDiskInput(t, disk, reference, input, int64(i+1))
			}
		})
	}
}

func TestDiskSnapshotRejectsTruncationCorruptionAndWrongPolicy(t *testing.T) {
	disk := testDiskProcessor(t, 4)
	reference := NewFireEngineRetention(24*time.Hour, 48*time.Hour, 4)
	compareDiskInput(t, disk, reference, streamInput{"detection", detection("N20", "600", "0", "0", "n", 5)}, 1)
	state := diskStateBytes(t, disk)
	corrupt := bytes.Clone(state)
	corrupt[len(corrupt)-1] ^= 1
	for _, bad := range [][]byte{nil, state[:len(state)/2], corrupt} {
		if restored, err := restoreDiskFireProcessor(context.Background(), t.TempDir(), 24*time.Hour, 48*time.Hour, 4, 1, bytes.NewReader(bad)); err == nil {
			restored.Close()
			t.Fatal("invalid snapshot was accepted")
		}
	}
	for _, wrong := range []struct {
		lateness, retention time.Duration
		max                 int
		sequence            int64
	}{
		{12 * time.Hour, 48 * time.Hour, 4, 1}, {24 * time.Hour, 72 * time.Hour, 4, 1},
		{24 * time.Hour, 48 * time.Hour, 5, 1}, {24 * time.Hour, 48 * time.Hour, 4, 2},
	} {
		_, err := restoreDiskFireProcessor(context.Background(), t.TempDir(), wrong.lateness, wrong.retention, wrong.max, wrong.sequence, bytes.NewReader(state))
		if !errors.Is(err, ErrIncompatibleState) {
			t.Fatal("snapshot with different policy/sequence was not rejected", err)
		}
	}
}

func TestDiskSnapshotRejectsMissingRowsAndDuplicateRows(t *testing.T) {
	disk := testDiskProcessor(t, 4)
	reference := NewFireEngineRetention(24*time.Hour, 48*time.Hour, 4)
	compareDiskInput(t, disk, reference, streamInput{"detection", detection("N20", "600", "0", "0", "n", 5)}, 1)
	z, err := gzip.NewReader(bytes.NewReader(diskStateBytes(t, disk)))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	z.Close()
	lines := bytes.Split(bytes.TrimSpace(plain), []byte("\n"))
	for _, body := range [][]byte{
		bytes.Join(append([][]byte{lines[0]}, lines[2:]...), []byte("\n")),
		bytes.Join(append(append([][]byte{}, lines[:2]...), lines[1:]...), []byte("\n")),
	} {
		var encoded bytes.Buffer
		writer := gzip.NewWriter(&encoded)
		writer.Write(body)
		writer.Close()
		if restored, err := restoreDiskFireProcessor(context.Background(), t.TempDir(), 24*time.Hour, 48*time.Hour, 4, 1, &encoded); err == nil {
			restored.Close()
			t.Fatal("incomplete or duplicate snapshot rows were accepted")
		}
	}
}

type failedSnapshotWriter struct{}

func (failedSnapshotWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestDiskSnapshotPropagatesWriterFailureWithoutChangingState(t *testing.T) {
	disk := testDiskProcessor(t, 4)
	before := disk.header()
	if err := disk.WriteState(context.Background(), failedSnapshotWriter{}, 0); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("snapshot writer failure was hidden", err)
	}
	if !reflect.DeepEqual(before, disk.header()) {
		t.Fatal("failed snapshot changed processing state")
	}
}

func TestDiskProcessorSequencesIncludeArchivedNoops(t *testing.T) {
	disk := testDiskProcessor(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := disk.ApplyTo(ctx, "quarantined", nil, 1, testNow, nil); !errors.Is(err, errWorkingState) || disk.lastSequence != 0 {
		t.Fatal("canceled processing advanced the state", err)
	}
	if err := disk.ApplyTo(context.Background(), "quarantined", nil, 2, testNow, nil); !errors.Is(err, errWorkingState) {
		t.Fatal("journal gap was hidden", err)
	}
	if err := disk.ApplyTo(context.Background(), "quarantined", nil, 1, testNow, nil); err != nil {
		t.Fatal(err)
	}
	state := diskStateBytes(t, disk)
	restored := restoreTestDisk(t, disk, state, 1)
	defer restored.Close()
	if restored.Active() != 0 || restored.lastSequence != 1 {
		t.Fatal("archived no-op sequence was not restored")
	}
	if err := restored.ApplyTo(context.Background(), "detection", detection("N20", "600", "0", "0", "n", 3), 2, testNow, nil); err != nil {
		t.Fatal(err)
	}
}
