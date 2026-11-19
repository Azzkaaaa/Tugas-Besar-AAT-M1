package m2

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func TestReplayJournalMatchesReferenceFinalsAndReasons(t *testing.T) {
	reference := NewFireEngineRetention(24*time.Hour, 48*time.Hour, 4)
	reference.KeepFinal = true
	var journal, output bytes.Buffer
	encoder := json.NewEncoder(&journal)
	wantErrors := map[string]int64{}
	inputs := longFeed(4)
	for i, input := range inputs {
		row := storage.StreamEntry{Sequence: int64(i + 1), Kind: input.kind, Payload: input.payload, ReceivedAt: testNow}
		if err := encoder.Encode(row); err != nil {
			t.Fatal(err)
		}
		if _, err := reference.Apply(row.Kind, row.Payload, row.Sequence, row.ReceivedAt); err != nil {
			wantErrors[err.Error()]++
		}
	}
	root := t.TempDir()
	summary, err := ReplayJournal(context.Background(), &journal, &output, ReplayOptions{WorkDir: root, Lateness: 24 * time.Hour, Retention: 48 * time.Hour, MaxDetections: 4})
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Items   []*domain.HazardRecord `json:"items"`
		Reasons map[string]int64       `json:"parked_reasons"`
	}
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(outputs(t, got.Items), outputs(t, reference.Snapshot())) || !reflect.DeepEqual(got.Reasons, wantErrors) {
		t.Fatal("production-disk replay differs from reference snapshot/reasons")
	}
	fires, singles := reference.Counts()
	if summary.InputEvents != int64(len(inputs)) || summary.ActiveFires != fires || summary.Singletons != singles || summary.Late != reference.Late || summary.ExpiredCorrections != reference.Expired || summary.Engine != "production-disk" || summary.P95ComputeSeconds <= 0 {
		t.Fatalf("summary differs: %+v", summary)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary state not cleaned: %v %v", entries, err)
	}
}

func TestReplayJournalRefusesMissingSequence(t *testing.T) {
	var journal, output bytes.Buffer
	encoder := json.NewEncoder(&journal)
	for _, sequence := range []int64{1, 3} {
		if err := encoder.Encode(storage.StreamEntry{Sequence: sequence, Kind: "heartbeat", Payload: []byte(`{}`), ReceivedAt: testNow}); err != nil {
			t.Fatal(err)
		}
	}
	_, err := ReplayJournal(context.Background(), &journal, &output, ReplayOptions{WorkDir: t.TempDir(), Lateness: 24 * time.Hour, Retention: 48 * time.Hour, MaxDetections: 100})
	if err == nil || !strings.Contains(err.Error(), "noncontiguous") || output.Len() != 0 {
		t.Fatalf("incomplete journal was accepted: output=%q err=%v", output.String(), err)
	}
}
