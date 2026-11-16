package m2

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestCoastalValidationAndCritical(t *testing.T) {
	stations := map[string]Station{"s": {ID: "s", Province: "Sumatera Barat", Latitude: -.9, Longitude: 100.3}}
	good := `{"observation_id":"o","station_id":"s","observed_at":"2026-11-12T12:00:00Z","sea_level_anomaly_m":0.2,"wave_height_m":1.0,"quality":"ok","critical":true}`
	h, err := observation([]byte(good), stations, testNow, testNow)
	if err != nil || h.Severity != "AWAS" || h.Source != "SENSOR_PANTAI" {
		t.Fatalf("%+v %v", h, err)
	}
	for _, bad := range []string{strings.Replace(good, `true`, `"true"`, 1), strings.Replace(good, `1.0`, `-1`, 1), strings.Replace(good, `"ok"`, `"bad"`, 1), strings.Replace(good, `"station_id":"s"`, `"station_id":"missing"`, 1), strings.Replace(good, `"wave_height_m":1.0,`, ``, 1), strings.Replace(good, `0.2`, `"0.2"`, 1)} {
		if _, err := observation([]byte(bad), stations, testNow, testNow); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}
func TestBreakerBoundsRequestsAndRecovers(t *testing.T) {
	b := Breaker{Threshold: 3, OpenFor: 10 * time.Second}
	now := time.Now()
	for i := 0; i < 3; i++ {
		if !b.Allow(now) {
			t.Fatal("opened too early")
		}
		b.Fail(now, fmt.Errorf("offline"))
	}
	for i := 0; i < 10000; i++ {
		if b.Allow(now.Add(9 * time.Second)) {
			t.Fatal("allowed request during outage")
		}
	}
	if !b.Allow(now.Add(10 * time.Second)) {
		t.Fatal("half-open probe never allowed")
	}
	b.Success()
	if b.Failures != 0 || !b.Allow(now) {
		t.Fatal("did not recover")
	}
	b.Fail(now, &statusError{Status: 429, RetryAfter: 30 * time.Second})
	if b.Allow(now.Add(20 * time.Second)) {
		t.Fatal("Retry-After ignored")
	}
}
func TestSSEFramingAndTruncatedReconnect(t *testing.T) {
	var frames []sseFrame
	err := ReadSSE(strings.NewReader(": keepalive\r\nid: 1\r\nevent: detection\r\ndata: {\r\ndata: \"x\":1}\r\n\r\nid: 2\ndata: incomplete"), 1024, func(f sseFrame) error { frames = append(frames, f); return nil })
	if err != io.EOF || len(frames) != 1 || string(frames[0].Data) != "{\n\"x\":1}" || frames[0].ID != "1" {
		t.Fatalf("%+v %v", frames, err)
	}
	if err := ReadSSE(strings.NewReader("data: "+strings.Repeat("x", 1025)+"\n\n"), 1024, func(sseFrame) error { t.Fatal("oversize delivered"); return nil }); err == nil {
		t.Fatal("accepted oversize")
	}
}
