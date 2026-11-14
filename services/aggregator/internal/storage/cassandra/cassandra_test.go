package cassandra

import (
	"testing"
	"time"
)

func TestSplitStatements(t *testing.T) {
	got := splitStatements("-- comment\nCREATE TABLE a (\n  x int PRIMARY KEY\n);\n\n-- more\nCREATE TABLE b (y int PRIMARY KEY)\n  WITH gc_grace_seconds = 1;\n")
	if len(got) != 2 || got[0] != "CREATE TABLE a (\n  x int PRIMARY KEY\n)" || got[1] != "CREATE TABLE b (y int PRIMARY KEY)\n  WITH gc_grace_seconds = 1" {
		t.Fatalf("splitStatements = %q", got)
	}
}

func TestEmbeddedMigrationsLoad(t *testing.T) {
	ms, err := loadMigrations()
	if err != nil || len(ms) == 0 || ms[0].version != 1 || len(ms[0].statements) < 8 {
		t.Fatalf("loadMigrations = %+v, %v", ms, err)
	}
}

func TestQuerySummary(t *testing.T) {
	cases := map[string]string{
		"SELECT a, b FROM hazards_by_id WHERE x = ?": "SELECT hazards_by_id",
		"INSERT INTO outbox_pending (a) VALUES (?)":  "INSERT outbox_pending",
		"UPDATE source_state SET a = ?":              "UPDATE source_state",
		"DELETE FROM outbox_pending WHERE a = ?":     "DELETE outbox_pending",
		"TRUNCATE x":                                 "TRUNCATE",
		"":                                           "CQL",
	}
	for stmt, want := range cases {
		if got := querySummary(stmt); got != want {
			t.Errorf("querySummary(%q) = %q, want %q", stmt, got, want)
		}
	}
}

func TestScheduleBucketOf(t *testing.T) {
	day := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	s := schedule{
		{From: scheduleOrigin, Period: time.Minute},
		{From: day.Add(2 * time.Hour), Period: time.Hour},
		{From: day.Add(5 * time.Hour), Period: 5 * time.Minute},
	}
	cases := []struct{ at, want time.Time }{
		{day.Add(time.Hour + 7*time.Minute + 59*time.Second), day.Add(time.Hour + 7*time.Minute)},
		{day.Add(2*time.Hour - time.Millisecond), day.Add(2*time.Hour - time.Minute)}, // last 1m bucket
		{day.Add(2 * time.Hour), day.Add(2 * time.Hour)},                              // first 1h bucket
		{day.Add(4*time.Hour + 59*time.Minute), day.Add(4 * time.Hour)},
		{day.Add(5*time.Hour + 9*time.Minute), day.Add(5*time.Hour + 5*time.Minute)},
		{time.Date(1960, 1, 1, 0, 0, 30, 0, time.UTC), time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)}, // before origin
	}
	for _, c := range cases {
		if got := s.bucketOf(c.at); !got.Equal(c.want) {
			t.Errorf("bucketOf(%s) = %s, want %s", c.at, got, c.want)
		}
	}
	// Non-UTC input lands in the same bucket.
	wib := day.Add(time.Hour + 7*time.Minute).In(time.FixedZone("WIB", 7*3600))
	if got := s.bucketOf(wib); !got.Equal(day.Add(time.Hour + 7*time.Minute)) {
		t.Errorf("bucketOf(WIB) = %s", got)
	}
	cur, next := s.current(day.Add(3 * time.Hour))
	if cur.Period != time.Hour || next == nil || next.Period != 5*time.Minute {
		t.Errorf("current = %+v, %+v", cur, next)
	}
}

func TestChangeBoundary(t *testing.T) {
	at := time.Date(2026, 3, 1, 10, 2, 30, 0, time.UTC)
	cases := []struct {
		a, b time.Duration
		want time.Time
	}{
		{time.Minute, 5 * time.Minute, time.Date(2026, 3, 1, 10, 5, 0, 0, time.UTC)},
		{5 * time.Minute, 3 * time.Minute, time.Date(2026, 3, 1, 10, 15, 0, 0, time.UTC)}, // lcm 15m
		{time.Hour, 45 * time.Minute, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)},       // lcm 3h
		{time.Minute, 24 * time.Hour, time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		got := changeBoundary(at, c.a, c.b)
		if !got.Equal(c.want) {
			t.Errorf("changeBoundary(%s, %s) = %s, want %s", c.a, c.b, got, c.want)
		}
		if !aligned(got, c.a) || !aligned(got, c.b) {
			t.Errorf("boundary %s not aligned to %s and %s", got, c.a, c.b)
		}
	}
	// A boundary exactly at earliest is kept.
	exact := time.Date(2026, 3, 1, 10, 5, 0, 0, time.UTC)
	if got := changeBoundary(exact, time.Minute, 5*time.Minute); !got.Equal(exact) {
		t.Errorf("changeBoundary(exact) = %s", got)
	}
}

func TestValidateBucketPeriod(t *testing.T) {
	for _, ok := range []time.Duration{time.Second, time.Minute, 5 * time.Minute, 45 * time.Minute, time.Hour, 24 * time.Hour} {
		if err := ValidateBucketPeriod(ok); err != nil {
			t.Errorf("%s rejected: %v", ok, err)
		}
	}
	for _, bad := range []time.Duration{0, 500 * time.Millisecond, 7 * time.Minute, 25 * time.Hour, 90 * time.Second / 7} {
		if err := ValidateBucketPeriod(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

func TestDecodeCursorRejectsIncomplete(t *testing.T) {
	if _, err := decodeCursor("e30"); err == nil { // "{}"
		t.Fatal("empty cursor object accepted")
	}
}

func aligned(t time.Time, d time.Duration) bool { return t.Unix()%int64(d/time.Second) == 0 }
