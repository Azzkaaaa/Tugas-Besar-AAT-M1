// Package reconcile compares source evidence with signed receiver receipts.
// SQLite keeps complete catalogs on disk instead of in an unbounded Go map.
package reconcile

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	_ "modernc.org/sqlite"
)

type Expected struct {
	ObservationID string    `json:"observation_id"`
	Critical      *bool     `json:"critical"`
	Valid         *bool     `json:"valid"`
	EmittedAt     time.Time `json:"emitted_at"`
}
type Options struct {
	PublicKey    ed25519.PublicKey
	ReceiverID   string
	Cutoff       time.Time
	Latency      time.Duration
	TimelyTarget float64
	TempDir      string
	Journal      io.Reader
}
type Report struct {
	Schema                       string    `json:"schema"`
	ReceiverID                   string    `json:"receiver_id"`
	Cutoff                       time.Time `json:"cutoff"`
	LatencyBasis                 string    `json:"latency_basis"`
	LatencyLimitSeconds          float64   `json:"latency_limit_seconds"`
	TimelyTarget                 float64   `json:"timely_target"`
	Expected                     int64     `json:"expected_critical"`
	Received                     int64     `json:"received_critical"`
	Missing                      int64     `json:"missing_critical"`
	Late                         int64     `json:"late_critical"`
	ClockAnomalies               int64     `json:"clock_anomalies"`
	InvalidReceipts              int64     `json:"invalid_receipts"`
	DuplicateReceipts            int64     `json:"duplicate_receipts"`
	UnexpectedReceipts           int64     `json:"unexpected_receipts"`
	ExcludedInvalid              int64     `json:"excluded_invalid_source_records"`
	TimelyFraction               float64   `json:"timely_fraction"`
	P95Seconds                   *float64  `json:"p95_seconds"`
	AllowedLateEvents            float64   `json:"allowed_late_events"`
	BudgetConsumedFraction       float64   `json:"timeliness_budget_consumed_fraction"`
	LossBudget                   int       `json:"loss_budget_events"`
	WindowComplete               bool      `json:"window_complete"`
	TargetMet                    bool      `json:"target_met"`
	MissingExamples              []string  `json:"missing_examples"`
	MissingExamplesTruncated     bool      `json:"missing_examples_truncated"`
	CanonicalRevisions           int64     `json:"canonical_critical_revisions"`
	MissingCanonicalRevisions    int64     `json:"missing_canonical_revisions"`
	PayloadMismatches            int64     `json:"receipt_payload_mismatches"`
	InvalidCanonicalObservations int64     `json:"invalid_observations_in_canonical_journal"`
	SnapshotVersions             []int64   `json:"snapshot_versions"`
}

func lines(input io.Reader, each func([]byte) error) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		if err := each(scanner.Bytes()); err != nil {
			return err
		}
	}
	return scanner.Err()
}
func nanos(t time.Time) (int64, error) {
	n := t.UnixNano()
	if t.IsZero() || !time.Unix(0, n).Equal(t) {
		return 0, errors.New("timestamp is missing or outside the supported range")
	}
	return n, nil
}

func Run(ctx context.Context, expected, receipts io.Reader, options Options) (Report, error) {
	r := Report{Schema: "bnpb-critical-reconciliation-v1", ReceiverID: options.ReceiverID, Cutoff: options.Cutoff.UTC(), LatencyBasis: "source_observation_emitted_at_to_first_verified_receiver_receipt", LatencyLimitSeconds: options.Latency.Seconds(), TimelyTarget: options.TimelyTarget, MissingExamples: []string{}}
	if len(options.PublicKey) != ed25519.PublicKeySize || options.ReceiverID == "" || options.Cutoff.IsZero() || options.Latency <= 0 || options.TimelyTarget <= 0 || options.TimelyTarget >= 1 {
		return r, errors.New("invalid reconciliation options")
	}
	if options.Journal == nil {
		return r, errors.New("complete canonical journal snapshot evidence is required")
	}
	file, err := os.CreateTemp(options.TempDir, "critical-reconciliation-*.db")
	if err != nil {
		return r, err
	}
	name := file.Name()
	file.Close()
	defer os.Remove(name)
	db, err := sql.Open("sqlite", name)
	if err != nil {
		return r, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, q := range []string{`PRAGMA journal_mode=OFF`, `PRAGMA synchronous=OFF`, `PRAGMA cache_size=-8192`, `CREATE TABLE expected (hazard_id TEXT PRIMARY KEY,observation_id TEXT NOT NULL,emitted_ns INTEGER NOT NULL)`, `CREATE TABLE received (hazard_id TEXT PRIMARY KEY,received_ns INTEGER NOT NULL)`, `CREATE TABLE received_events (event_id TEXT PRIMARY KEY,hazard_id TEXT NOT NULL,payload_sha256 TEXT NOT NULL,received_ns INTEGER NOT NULL)`, `CREATE TABLE canonical (event_id TEXT PRIMARY KEY,hazard_id TEXT NOT NULL,payload_sha256 TEXT NOT NULL,shard INTEGER NOT NULL,generation INTEGER NOT NULL,UNIQUE(shard,generation))`, `CREATE TABLE invalid_expected (hazard_id TEXT PRIMARY KEY)`} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return r, err
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	var latest time.Time
	err = lines(expected, func(raw []byte) error {
		var e Expected
		if json.Unmarshal(raw, &e) != nil || e.Critical == nil || e.Valid == nil {
			return errors.New("expected source evidence requires explicit critical and valid flags")
		}
		if !*e.Valid {
			if *e.Critical {
				r.ExcludedInvalid++
				if e.ObservationID != "" {
					_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO invalid_expected VALUES (?)`, domain.HazardID(domain.SourceCoastal, e.ObservationID))
					if err != nil {
						return err
					}
				}
			}
			return nil
		}
		if !*e.Critical {
			return nil
		}
		if e.ObservationID == "" {
			return errors.New("expected critical observation has no identity")
		}
		n, err := nanos(e.EmittedAt)
		if err != nil {
			return fmt.Errorf("source emitted_at evidence required: %w", err)
		}
		if e.EmittedAt.After(options.Cutoff) {
			return errors.New("source evidence extends beyond the declared cutoff")
		}
		if e.EmittedAt.After(latest) {
			latest = e.EmittedAt
		}
		id := domain.HazardID(domain.SourceCoastal, e.ObservationID)
		if _, err = tx.ExecContext(ctx, `INSERT INTO expected (hazard_id,observation_id,emitted_ns) VALUES (?,?,?) ON CONFLICT DO NOTHING`, id, e.ObservationID, n); err != nil {
			return err
		}
		var existing int64
		if err = tx.QueryRowContext(ctx, `SELECT emitted_ns FROM expected WHERE hazard_id=?`, id).Scan(&existing); err != nil {
			return err
		}
		if existing != n {
			return errors.New("ambiguous emitted_at for the same source observation")
		}
		return nil
	})
	if err != nil {
		return r, err
	}
	err = lines(receipts, func(raw []byte) error {
		var receipt storage.CriticalReceipt
		if json.Unmarshal(raw, &receipt) != nil || criticaldelivery.VerifyReceipt(options.PublicKey, options.ReceiverID, receipt) != nil {
			r.InvalidReceipts++
			return nil
		}
		if receipt.ReceivedAt.After(options.Cutoff) {
			return nil
		}
		n, err := nanos(receipt.ReceivedAt)
		if err != nil {
			r.InvalidReceipts++
			return nil
		}
		var existingHash, existingHazard string
		err = tx.QueryRowContext(ctx, `SELECT payload_sha256,hazard_id FROM received_events WHERE event_id=?`, receipt.EventID).Scan(&existingHash, &existingHazard)
		if err == nil {
			r.DuplicateReceipts++
			if existingHash != receipt.PayloadSHA256 || existingHazard != receipt.HazardID {
				r.InvalidReceipts++
				return nil
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO received_events (event_id,hazard_id,payload_sha256,received_ns) VALUES (?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET received_ns=MIN(received_events.received_ns,excluded.received_ns)`, receipt.EventID, receipt.HazardID, receipt.PayloadSHA256, n)
		return err
	})
	if err != nil {
		return r, err
	}
	var manifest criticaldelivery.Manifest
	seenManifest := false
	err = lines(options.Journal, func(raw []byte) error {
		var envelope struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(raw, &envelope) != nil {
			return errors.New("invalid canonical journal evidence")
		}
		if seenManifest {
			return errors.New("canonical evidence follows its completion manifest")
		}
		if envelope.Kind != "" {
			if envelope.Kind != "critical-journal-manifest" || json.Unmarshal(raw, &manifest) != nil || !manifest.Complete {
				return errors.New("canonical journal is incomplete")
			}
			seenManifest = true
			return nil
		}
		var e storage.CriticalEvent
		if json.Unmarshal(raw, &e) != nil || e.Generation < 1 || e.Shard < 0 || e.Shard >= 16 {
			return errors.New("invalid canonical journal event")
		}
		hash := sha256.Sum256(e.Payload)
		if hex.EncodeToString(hash[:]) != e.PayloadSHA256 {
			return errors.New("canonical payload hash mismatch")
		}
		var body struct {
			EventID string              `json:"event_id"`
			Data    domain.HazardRecord `json:"data"`
		}
		if json.Unmarshal(e.Payload, &body) != nil || body.EventID != e.EventID || body.Data.HazardID != e.HazardID || body.Data.SourceRefID != e.SourceRefID || e.HazardID != domain.HazardID(domain.SourceCoastal, e.SourceRefID) {
			return errors.New("canonical observation identity mismatch")
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO canonical VALUES (?,?,?,?,?)`, e.EventID, e.HazardID, e.PayloadSHA256, e.Shard, e.Generation)
		return err
	})
	if err != nil {
		return r, err
	}
	if !seenManifest {
		return r, errors.New("canonical journal has no completion manifest")
	}
	total, err := criticaldelivery.VersionCount(manifest.Versions)
	if err != nil {
		return r, err
	}
	var actual int64
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM canonical`).Scan(&actual); err != nil {
		return r, err
	}
	if actual != total || actual != manifest.Count {
		return r, errors.New("canonical snapshot row count does not match its committed generations")
	}
	for shard, version := range manifest.Versions {
		var n int64
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM canonical WHERE shard=? AND generation BETWEEN 1 AND ?`, shard, version).Scan(&n); err != nil {
			return r, err
		}
		if n != version {
			return r, errors.New("canonical snapshot has missing generations")
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO received SELECT c.hazard_id,MIN(r.received_ns) FROM canonical c JOIN received_events r ON c.event_id=r.event_id AND c.hazard_id=r.hazard_id AND c.payload_sha256=r.payload_sha256 GROUP BY c.hazard_id`); err != nil {
		return r, err
	}
	r.CanonicalRevisions = actual
	r.SnapshotVersions = manifest.Versions
	if err = tx.Commit(); err != nil {
		return r, err
	}
	count := func(query string, args ...any) (int64, error) {
		var n int64
		err := db.QueryRowContext(ctx, query, args...).Scan(&n)
		return n, err
	}
	if r.Expected, err = count(`SELECT COUNT(*) FROM expected`); err != nil {
		return r, err
	}
	if r.Expected == 0 {
		return r, errors.New("no expected critical source events; success cannot be inferred from empty evidence")
	}
	if r.Received, err = count(`SELECT COUNT(*) FROM expected e JOIN received r USING(hazard_id)`); err != nil {
		return r, err
	}
	r.Missing = r.Expected - r.Received
	if r.Late, err = count(`SELECT COUNT(*) FROM expected e JOIN received r USING(hazard_id) WHERE r.received_ns-e.emitted_ns>?`, options.Latency.Nanoseconds()); err != nil {
		return r, err
	}
	if r.ClockAnomalies, err = count(`SELECT COUNT(*) FROM expected e JOIN received r USING(hazard_id) WHERE r.received_ns<e.emitted_ns`); err != nil {
		return r, err
	}
	if r.UnexpectedReceipts, err = count(`SELECT COUNT(*) FROM received r LEFT JOIN expected e USING(hazard_id) WHERE e.hazard_id IS NULL`); err != nil {
		return r, err
	}
	if r.MissingCanonicalRevisions, err = count(`SELECT COUNT(*) FROM canonical c LEFT JOIN received_events r ON c.event_id=r.event_id WHERE r.event_id IS NULL`); err != nil {
		return r, err
	}
	if r.PayloadMismatches, err = count(`SELECT COUNT(*) FROM canonical c JOIN received_events r USING(event_id) WHERE c.hazard_id<>r.hazard_id OR c.payload_sha256<>r.payload_sha256`); err != nil {
		return r, err
	}
	if r.InvalidCanonicalObservations, err = count(`SELECT COUNT(*) FROM canonical c JOIN invalid_expected e USING(hazard_id)`); err != nil {
		return r, err
	}
	timely := r.Received - r.Late - r.ClockAnomalies
	r.TimelyFraction = float64(timely) / float64(r.Expected)
	r.AllowedLateEvents = (1 - options.TimelyTarget) * float64(r.Expected)
	r.BudgetConsumedFraction = float64(r.Expected-timely) / r.AllowedLateEvents
	valid := r.Received - r.ClockAnomalies
	if valid > 0 {
		offset := int64(math.Ceil(.95*float64(valid))) - 1
		var latency int64
		if err = db.QueryRowContext(ctx, `SELECT r.received_ns-e.emitted_ns FROM expected e JOIN received r USING(hazard_id) WHERE r.received_ns>=e.emitted_ns ORDER BY r.received_ns-e.emitted_ns LIMIT 1 OFFSET ?`, offset).Scan(&latency); err != nil {
			return r, err
		}
		seconds := float64(latency) / float64(time.Second)
		r.P95Seconds = &seconds
	}
	r.WindowComplete = !latest.Add(options.Latency).After(options.Cutoff) && !manifest.CapturedAt.Before(options.Cutoff)
	r.TargetMet = r.WindowComplete && r.Missing == 0 && r.MissingCanonicalRevisions == 0 && r.PayloadMismatches == 0 && r.InvalidCanonicalObservations == 0 && r.InvalidReceipts == 0 && r.ClockAnomalies == 0 && r.P95Seconds != nil && *r.P95Seconds <= options.Latency.Seconds() && r.TimelyFraction+1e-12 >= options.TimelyTarget
	rows, err := db.QueryContext(ctx, `SELECT e.observation_id FROM expected e LEFT JOIN received r USING(hazard_id) WHERE r.hazard_id IS NULL ORDER BY e.observation_id LIMIT 100`)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return r, err
		}
		r.MissingExamples = append(r.MissingExamples, id)
	}
	r.MissingExamplesTruncated = r.Missing > int64(len(r.MissingExamples))
	return r, rows.Err()
}
