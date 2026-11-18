package m2

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

type streamInput struct {
	kind    string
	payload []byte
}

func datedDetection(satellite string, at time.Time, lat, lon float64, confidence string, frp float64) []byte {
	return []byte(fmt.Sprintf(`{"satellite":%q,"acq_date":%q,"acq_time":"%s","latitude":"%.4f","longitude":"%.4f","confidence":%q,"frp":%g}`,
		satellite, at.Format("2006-01-02"), at.Format("1504"), lat, lon, confidence, frp))
}

// longFeed spans many days: pairs of detections form short-lived fires at
// distinct places, with corrections, retractions and late input mixed in.
func longFeed(days int) []streamInput {
	var in []streamInput
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	n := 0
	for h := 0; h < days*24; h++ {
		at := start.Add(time.Duration(h) * time.Hour)
		lat, lon := -6+float64(h%50)*0.05, 106+float64(h/50)*0.05
		a := datedDetection("N20", at, lat, lon, "n", 5)
		b := datedDetection("N21", at.Add(10*time.Minute), lat, lon+0.005, "h", 7)
		in = append(in, streamInput{"detection", a}, streamInput{"detection", b})
		if h%7 == 0 {
			in = append(in, streamInput{"detection", datedDetection("N20", at, lat+0.3, lon, "l", 1)})
		}
		if h%11 == 0 {
			o, _ := decodeObject(b)
			d, _ := parseDetection(o)
			in = append(in, streamInput{"update", []byte(fmt.Sprintf(`{"key":%q,"changes":{"frp":40}}`, d.Key))})
		}
		if h%13 == 0 {
			o, _ := decodeObject(a)
			d, _ := parseDetection(o)
			in = append(in, streamInput{"retract", []byte(fmt.Sprintf(`{"key":%q}`, d.Key))})
		}
		if h%17 == 0 {
			in = append(in, streamInput{"detection", datedDetection("N20", at.Add(-40*time.Hour), lat, lon, "n", 2)})
		}
		in = append(in, streamInput{"heartbeat", []byte(fmt.Sprintf(`{"virtual_now":%q}`, at.Add(30*time.Minute).Format(time.RFC3339)))})
		n++
	}
	return in
}

func outputs(t *testing.T, rows []*domain.HazardRecord) []string {
	t.Helper()
	var out []string
	for _, h := range rows {
		b, err := json.Marshal(h)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(b))
	}
	return out
}

func TestFireStateStaysBoundedOverLongFeed(t *testing.T) {
	e := NewFireEngineRetention(24*time.Hour, 0, 1000)
	in := longFeed(30)
	peak := 0
	for i, x := range in {
		_, err := e.Apply(x.kind, x.payload, int64(i+1), testNow)
		if err != nil && err.Error() != "late_beyond_allowed_lateness" {
			t.Fatalf("input %d: %v", i, err)
		}
		peak = max(peak, e.Active())
	}
	// 30 days of hourly fires, but only Lateness+6h (+ one compaction step) stay live.
	if peak > 2*31*3+10 {
		t.Fatalf("active state grew with stream length: peak %d", peak)
	}
	if e.OverCapacity != 0 {
		t.Fatalf("over capacity: %d", e.OverCapacity)
	}
	if e.Late == 0 {
		t.Fatal("late input was not reported")
	}
}

func TestFireSnapshotRestoreMatchesUninterruptedReplay(t *testing.T) {
	in := longFeed(6)
	reference := NewFireEngineRetention(24*time.Hour, 36*time.Hour, 1000)
	reference.KeepFinal = true
	var want []string
	for i, x := range in {
		rows, _ := reference.Apply(x.kind, x.payload, int64(i+1), testNow)
		want = append(want, outputs(t, rows)...)
	}
	for _, every := range []int{1, 37, 250} {
		e := NewFireEngineRetention(24*time.Hour, 36*time.Hour, 1000)
		var got []string
		for i, x := range in {
			if i > 0 && i%every == 0 {
				state, err := e.EncodeState()
				if err != nil {
					t.Fatal(err)
				}
				if e, err = RestoreFireEngine(state, 24*time.Hour, 36*time.Hour, 1000); err != nil {
					t.Fatal(err)
				}
			}
			rows, _ := e.Apply(x.kind, x.payload, int64(i+1), testNow)
			got = append(got, outputs(t, rows)...)
		}
		if len(got) != len(want) {
			t.Fatalf("every %d: %d revisions, uninterrupted %d", every, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("every %d: revision %d differs\n got %s\nwant %s", every, i, got[i], want[i])
			}
		}
		if e.Late != reference.Late || e.Expired != reference.Expired || e.Low != reference.Low || e.Active() != reference.Active() {
			t.Fatalf("every %d: counters differ", every)
		}
	}
}

func TestFireRestoreRejectsDifferentPolicy(t *testing.T) {
	e := NewFireEngineRetention(24*time.Hour, 48*time.Hour, 10)
	state, err := e.EncodeState()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = RestoreFireEngine(state, 12*time.Hour, 48*time.Hour, 10); err == nil {
		t.Fatal("snapshot from another lateness policy was accepted")
	}
}

func TestFireCorrectionBeyondRetentionIsReported(t *testing.T) {
	e := NewFireEngineRetention(time.Hour, 0, 100)
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	a := datedDetection("N20", start, 0, 0, "n", 5)
	b := datedDetection("N21", start.Add(time.Minute), 0, 0.005, "n", 5)
	for i, x := range [][]byte{a, b} {
		if _, err := e.Apply("detection", x, int64(i+1), testNow); err != nil {
			t.Fatal(err)
		}
	}
	far := datedDetection("N20", start.Add(30*time.Hour), 5, 5, "n", 5)
	if _, err := e.Apply("detection", far, 3, testNow); err != nil {
		t.Fatal(err)
	}
	o, _ := decodeObject(a)
	d, _ := parseDetection(o)
	_, err := e.Apply("retract", []byte(fmt.Sprintf(`{"key":%q}`, d.Key)), 4, testNow)
	if err == nil || err.Error() != "retract_beyond_state_retention" || e.Expired != 1 {
		t.Fatalf("expired retraction was not reported: %v (expired %d)", err, e.Expired)
	}
}

func TestFireSoftCapacityNeverStopsProcessing(t *testing.T) {
	e := NewFireEngineRetention(24*time.Hour, 0, 4)
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		x := datedDetection("N20", start.Add(time.Duration(i)*time.Minute), float64(i), 0, "n", 1)
		if _, err := e.Apply("detection", x, int64(i+1), testNow); err != nil {
			t.Fatalf("input %d stopped processing: %v", i, err)
		}
	}
	if e.OverCapacity == 0 {
		t.Fatal("exceeding the soft bound was not reported")
	}
}
