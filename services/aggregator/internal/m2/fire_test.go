package m2

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

var testNow = time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)

func detection(satellite, clock, lat, lon, confidence string, frp float64) []byte {
	return []byte(fmt.Sprintf(`{"satellite":%q,"acq_date":"2026-11-12","acq_time":%s,"latitude":%s,"longitude":%s,"confidence":%q,"frp":%g}`, satellite, clock, lat, lon, confidence, frp))
}
func applyOK(t *testing.T, e *FireEngine, kind string, b []byte, n int64) []*domain.HazardRecord {
	t.Helper()
	rows, err := e.Apply(kind, b, n, testNow)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}
func keyOf(t *testing.T, b []byte) string {
	t.Helper()
	o, err := decodeObject(b)
	if err != nil {
		t.Fatal(err)
	}
	d, err := parseDetection(o)
	if err != nil {
		t.Fatal(err)
	}
	return d.Key
}
func TestFireChainDedupAndCorrections(t *testing.T) {
	e := NewFireEngine(24*time.Hour, 1000)
	a := detection("N20", "600", "0.0000", "0.0000", "n", 10)
	b := detection("N21", "700", "0.0000", "0.0080", "h", 20)
	c := detection("N20", "800", "0.0000", "0.0160", "n", 3)
	if rows := applyOK(t, e, "detection", a, 1); len(rows) != 0 {
		t.Fatal("singleton became a fire")
	}
	rows := applyOK(t, e, "detection", b, 2)
	if len(rows) != 1 {
		t.Fatalf("want one fire: %v", rows)
	}
	id := rows[0].SourceRefID
	if rows[0].Severity != domain.SeveritySiaga || rows[0].Attributes["max_confidence"] != "h" {
		t.Fatal(rows[0])
	}
	rows = applyOK(t, e, "detection", c, 3)
	if len(rows) != 1 || rows[0].SourceRefID != id || rows[0].Attributes["detection_count"] != 3 {
		t.Fatal("chain was not preserved")
	}
	if got := rows[0].Attributes["detection_keys"].([]string)[0]; got != "N20|2026-11-12|600|0.0000|0.0000" {
		t.Fatalf("numeric lexemes changed: %s", got)
	}
	if rows := applyOK(t, e, "detection", a, 4); len(rows) != 0 {
		t.Fatal("duplicate changed fire")
	}
	patch, _ := json.Marshal(map[string]any{"key": keyOf(t, a), "changes": map[string]any{"frp": 100}})
	rows = applyOK(t, e, "update", patch, 5)
	if len(rows) != 1 || rows[0].SourceRefID != id || rows[0].Severity != domain.SeverityAwas {
		t.Fatal("update replaced identity")
	}
	remove, _ := json.Marshal(map[string]string{"key": keyOf(t, b)})
	rows = applyOK(t, e, "retract", remove, 6)
	if len(rows) != 1 || !domain.Withdrawn(rows[0]) || rows[0].SourceRefID != id {
		t.Fatal("removing bridge must withdraw old fire")
	}
	fires, singles := e.Counts()
	if fires != 0 || singles != 2 {
		t.Fatalf("%d fires, %d singletons", fires, singles)
	}
	rows = applyOK(t, e, "update", b, 7)
	if len(rows) != 1 || rows[0].SourceRefID != id || domain.Withdrawn(rows[0]) {
		t.Fatal("recovery must reactivate same fire")
	}
}
func TestFireEventTimeLowAndLate(t *testing.T) {
	e := NewFireEngine(time.Hour, 1000)
	a := detection("N20", "600", "0", "0", "n", 1)
	applyOK(t, e, "detection", a, 1)
	applyOK(t, e, "detection", detection("N21", "1200", "0", "0", "n", 1), 2)
	if fires, _ := e.Counts(); fires != 1 {
		t.Fatal("exact 360 minute boundary must connect")
	}
	applyOK(t, e, "detection", detection("N21", "1201", "0", "0.008", "l", 999), 3)
	if e.Low != 1 {
		t.Fatal("low confidence not counted")
	}
	late := detection("N20", "100", "0", "0", "n", 1)
	if _, err := e.Apply("detection", late, 4, testNow); err == nil || e.Late != 1 {
		t.Fatal("late input must be visible")
	}
	patch, _ := json.Marshal(map[string]any{"key": keyOf(t, a), "changes": map[string]any{"frp": 99}})
	if rows := applyOK(t, e, "update", patch, 5); len(rows) != 1 {
		t.Fatal("old known corrections must still apply")
	}
}
func TestFireRecoveryIsDeterministic(t *testing.T) {
	inputs := [][]byte{detection("N20", "600", "1", "100", "n", 4), detection("N21", "610", "1.001", "100", "h", 8), detection("N20", "620", "1.002", "100", "n", 20)}
	e := NewFireEngine(24*time.Hour, 1000)
	for i, b := range inputs {
		applyOK(t, e, "detection", b, int64(i+1))
	}
	recovered := NewFireEngine(24*time.Hour, 1000)
	for i, b := range inputs {
		applyOK(t, recovered, "detection", b, int64(i+1))
	}
	if !reflect.DeepEqual(e.latest, recovered.latest) {
		t.Fatal("replay changed final records")
	}
}
func TestFireSplitAndMerge(t *testing.T) {
	e := NewFireEngine(24*time.Hour, 1000)
	var bridge []byte
	for i := 0; i < 5; i++ {
		b := detection(fmt.Sprintf("N%d", i), "600", "0", fmt.Sprintf("%.4f", float64(i)*.008), "n", 1)
		applyOK(t, e, "detection", b, int64(i+1))
		if i == 2 {
			bridge = b
		}
	}
	before := sortedKeys(e.groups)[0]
	b, _ := json.Marshal(map[string]string{"key": keyOf(t, bridge)})
	applyOK(t, e, "retract", b, 6)
	fires, _ := e.Counts()
	if fires != 2 {
		t.Fatal("split must have two real components")
	}
	if !e.published[before] {
		t.Fatal("lost old identity")
	}
	applyOK(t, e, "update", bridge, 7)
	fires, _ = e.Counts()
	if fires != 1 {
		t.Fatal("merge left duplicate fires")
	}
}
func TestInputRejectsMalformed(t *testing.T) {
	for _, raw := range [][]byte{[]byte(`{"satellite":`), []byte("{\"x\":\"\xff\"}"), []byte(`null`), []byte(`{} {}`)} {
		if _, err := decodeObject(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
func BenchmarkIndependentDetections(b *testing.B) {
	for n := 0; n < b.N; n++ {
		e := NewFireEngine(48*time.Hour, 20000)
		for i := 0; i < 10000; i++ {
			raw := detection("N20", "600", fmt.Sprintf("%.5f", float64(i%100)*.02), fmt.Sprintf("%.5f", 100+float64(i/100)*.02), "n", 1)
			if _, err := e.Apply("detection", raw, int64(i+1), testNow); err != nil {
				b.Fatal(err)
			}
		}
	}
}
