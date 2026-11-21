// Package cassandra is the Apache Cassandra backend of the canonical store.
//
// Hazards are kept in three query tables written together in logged batches: by id, by source
// reference (identity, claimed with a lightweight transaction) and by time bucket (List). The
// outbox has a single pending partition and a single consumer (Backend.OutboxSingleConsumer).
package cassandra

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type Config struct {
	Hosts        []string
	Keyspace     string
	Username     string
	Password     string
	NodeFromRack bool
	Consistency  string // e.g. LOCAL_QUORUM
	// LocalDatacenter, when set, routes queries to that datacenter's nodes first, so LOCAL_*
	// consistency means that datacenter. Set it with NetworkTopologyStrategy.
	LocalDatacenter string
	// Replication applies when Open creates the keyspace.
	Replication Replication
	// ExistingOnly opens a keyspace that must already exist with its schema up to date (no
	// keyspace creation, migrations or schedule creation). Admin tools use it so a wrong
	// keyspace name fails instead of creating an empty keyspace.
	ExistingOnly bool
	// BucketPeriod is the initial span of one hazards_by_bucket partition, used when the keyspace
	// has no bucket schedule yet. It must divide 24h. Change it later with SetBucketPeriod
	// (cmd/storeadmin); a different value here then only logs a warning.
	BucketPeriod time.Duration
	// OutboxRetention is how long published and dead-lettered events stay in outbox_events.
	OutboxRetention time.Duration
}

// DB is an open Cassandra store.
type DB struct {
	fencedSchema bool
	session      *gocql.Session
	keyspace     string
	nodeFromRack bool
	retention    time.Duration
	summaryOnce  sync.Once
	summarySlot  chan struct{}
	// initialPeriod is the configured bucket period for a keyspace without a schedule.
	initialPeriod time.Duration
	sched         scheduleHolder
	// stop ends the schedule refresh.
	stop context.CancelFunc
}

// metaBucketPeriod is where builds before the bucket schedule recorded the fixed period.
const metaBucketPeriod = "bucket_period"

// A quorum read over separate tailnet hosts can exceed a LAN-sized 250ms
// budget. The parent request deadline still caps the entire operation.
const readUnitTimeout = time.Second

//go:embed migrations/*.cql
var migrationFiles embed.FS

// Open connects, creates the keyspace when missing, applies migrations and checks the bucket
// period against the one the keyspace was created with.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("no cassandra hosts configured")
	}
	if cfg.Keyspace == "" {
		return nil, fmt.Errorf("no cassandra keyspace configured")
	}
	if !validIdentifier(cfg.Keyspace) {
		return nil, fmt.Errorf("invalid cassandra keyspace name %q", cfg.Keyspace)
	}
	consistency, err := gocql.ParseConsistencyWrapper(defaultString(cfg.Consistency, "LOCAL_QUORUM"))
	if err != nil {
		return nil, err
	}
	replication, err := cfg.Replication.normalized()
	if err != nil {
		return nil, err
	}

	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Consistency = consistency
	cluster.SerialConsistency = gocql.LocalSerial
	// Never let a skewed Aggregator wall clock assign future LWW timestamps
	// that can outvote storage-controlled conditional mutations.
	cluster.DefaultTimestamp = false
	// Keyspace creation includes schema propagation and can exceed an ordinary
	// request's budget on the required 1 GiB store. This bootstrap session is
	// closed before opening the normal five-second application session below.
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	// A host selection policy holds per-session state, so each session gets its own.
	newPolicy := func() {
		if cfg.LocalDatacenter != "" {
			cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(cfg.LocalDatacenter))
		}
	}
	newPolicy()
	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: cfg.Username, Password: cfg.Password}
	}
	obs := newObserver()
	cluster.QueryObserver = obs
	cluster.BatchObserver = obs

	// The keyspace must exist before a session can use it.
	boot, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to cassandra: %w", err)
	}
	err = prepareKeyspace(ctx, boot, cfg.Keyspace, replication, cfg.ExistingOnly)
	boot.Close()
	if err != nil {
		return nil, err
	}

	cluster.Timeout = 5 * time.Second
	cluster.Keyspace = cfg.Keyspace
	newPolicy()
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("unable to open keyspace %s: %w", cfg.Keyspace, err)
	}
	db := &DB{session: session, keyspace: cfg.Keyspace, retention: cfg.OutboxRetention, initialPeriod: cfg.BucketPeriod, nodeFromRack: cfg.NodeFromRack}
	if cfg.ExistingOnly {
		err = db.openExisting(ctx)
	} else {
		err = db.migrate(ctx)
		if err == nil {
			err = db.initSchedule(ctx, cfg.BucketPeriod)
		}
	}
	if err != nil {
		session.Close()
		return nil, err
	}
	db.fencedSchema = true
	refreshCtx, stop := context.WithCancel(context.Background())
	db.stop = stop
	go db.refreshSchedule(refreshCtx)
	if !cfg.ExistingOnly {
		go db.maintainAudit(refreshCtx)
	}
	return db, nil
}

// Backend returns the storage ports served by db.
func (db *DB) Backend() *storage.Backend {
	return &storage.Backend{
		Critical:             criticalJournal{db},
		Analytics:            audit{db},
		Name:                 "cassandra",
		Hazards:              hazards{db},
		Sources:              sources{db},
		Outbox:               &outbox{db: db},
		Streams:              streams{db},
		Snapshots:            snapshots{db},
		Reports:              fieldReports{db},
		Inbox:                ingestInbox{db},
		Audit:                audit{db},
		Placement:            audit{db},
		OutboxSingleConsumer: true,
		Close:                db.Close,
	}
}

// prepareKeyspace creates the keyspace with replication when missing, unless existingOnly, in
// which case a missing keyspace is an error. An existing keyspace whose replication differs from
// the configured one is left alone with a warning: changing it needs ALTER KEYSPACE and a repair.
func prepareKeyspace(ctx context.Context, session *gocql.Session, keyspace string, replication Replication, existingOnly bool) error {
	actual, err := keyspaceReplication(ctx, session, keyspace)
	if err != nil {
		return fmt.Errorf("unable to read keyspace %s: %w", keyspace, err)
	}
	if actual != nil {
		if diff := replicationMismatch(actual, replication); diff != "" && !existingOnly {
			slog.Warn("keyspace replication differs from configuration; existing keyspaces are not changed automatically",
				"difference", diff, "keyspace", keyspace, "apply", fmt.Sprintf("ALTER KEYSPACE %s WITH replication = %s; then nodetool repair -full %s on every node", keyspace, replication.cql(), keyspace))
		}
		return nil
	}
	if existingOnly {
		return fmt.Errorf("keyspace %s does not exist (check CANONICAL_STORE_CASSANDRA_KEYSPACE; the aggregator creates it on first start)", keyspace)
	}
	err = session.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s`, keyspace, replication.cql())).
		WithContext(ctx).Exec()
	if err == nil {
		err = session.AwaitSchemaAgreement(ctx)
	}
	if err != nil {
		return fmt.Errorf("unable to create keyspace %s: %w", keyspace, err)
	}
	slog.Info("created keyspace", "keyspace", keyspace, "replication", replication.cql())
	return nil
}

// openExisting checks that every embedded migration was applied and loads the schedule, without
// writing anything.
func (db *DB) openExisting(ctx context.Context) error {
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	applied := map[int]bool{}
	iter := db.session.Query(`SELECT version FROM schema_migrations`).WithContext(ctx).Iter()
	var v int
	for iter.Scan(&v) {
		applied[v] = true
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("keyspace %s has no aggregator schema: %w", db.keyspace, err)
	}
	for _, m := range migrations {
		if !applied[m.version] {
			return fmt.Errorf("keyspace %s lacks migration %s: start the aggregator (this build) once to apply it", db.keyspace, m.name)
		}
	}
	s, err := db.loadSchedule(ctx)
	if err != nil {
		return err
	}
	if len(s) == 0 {
		return fmt.Errorf("keyspace %s has no bucket schedule: start the aggregator once to create it", db.keyspace)
	}
	db.sched.set(s)
	return nil
}

// Close stops the schedule refresh and closes the session.
func (db *DB) Close() {
	if db.stop != nil {
		db.stop()
	}
	db.session.Close()
}

// ---- migrations ---------------------------------------------------------------------------

const (
	migrationLockTTL  = 60 // seconds
	migrationLockWait = 2 * time.Minute
)

// migrate applies the embedded migrations not yet recorded in schema_migrations, in order, under
// a lease row so replicas starting together apply each once.
func (db *DB) migrate(ctx context.Context) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS schema_migrations (version int PRIMARY KEY, name text, applied_at timestamp)`,
		`CREATE TABLE IF NOT EXISTS schema_lock (name text PRIMARY KEY, holder text)`,
		`CREATE TABLE IF NOT EXISTS store_meta (key text PRIMARY KEY, value text)`,
	} {
		if err := db.session.Query(stmt).WithContext(ctx).Exec(); err != nil {
			return err
		}
	}
	if err := db.session.AwaitSchemaAgreement(ctx); err != nil {
		return err
	}

	holder := lockHolder()
	if err := db.acquireMigrationLock(ctx, holder); err != nil {
		return err
	}
	// Keep the lock while migrations run: a data migration can outlast its TTL.
	renewCtx, stopRenew := context.WithCancel(ctx)
	defer stopRenew()
	go db.renewMigrationLock(renewCtx, holder)
	defer func() {
		_, _ = db.session.Query(`DELETE FROM schema_lock WHERE name = 'migrate' IF holder = ?`, holder).
			WithContext(context.Background()).MapScanCAS(map[string]any{})
	}()

	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	applied := map[int]bool{}
	iter := db.session.Query(`SELECT version FROM schema_migrations`).WithContext(ctx).Iter()
	var v int
	for iter.Scan(&v) {
		applied[v] = true
	}
	if err := iter.Close(); err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		for _, stmt := range m.statements {
			if err := db.session.Query(stmt).WithContext(ctx).Exec(); err != nil {
				return fmt.Errorf("migration %s: %w", m.name, err)
			}
			if err := db.session.AwaitSchemaAgreement(ctx); err != nil {
				return err
			}
		}
		if m.run != nil {
			if err := m.run(db, ctx); err != nil {
				return fmt.Errorf("migration %s: %w", m.name, err)
			}
		}
		if err := db.session.Query(`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`,
			m.version, m.name, time.Now().UTC()).WithContext(ctx).Exec(); err != nil {
			return err
		}
		slog.Info("applied migration", "migration", m.name)
	}
	return nil
}

func (db *DB) acquireMigrationLock(ctx context.Context, holder string) error {
	deadline := time.Now().Add(migrationLockWait)
	for {
		applied, err := db.session.Query(`INSERT INTO schema_lock (name, holder) VALUES ('migrate', ?) IF NOT EXISTS USING TTL `+
			strconv.Itoa(migrationLockTTL), holder).WithContext(ctx).MapScanCAS(map[string]any{})
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for the migration lock")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// renewMigrationLock extends the migration lock until ctx is done.
func (db *DB) renewMigrationLock(ctx context.Context, holder string) {
	ticker := time.NewTicker(migrationLockTTL * time.Second / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := db.session.Query(`UPDATE schema_lock USING TTL `+strconv.Itoa(migrationLockTTL)+
				` SET holder = ? WHERE name = 'migrate' IF holder = ?`, holder, holder).
				WithContext(ctx).MapScanCAS(map[string]any{})
			if err != nil && ctx.Err() == nil {
				slog.Warn("renewing the migration lock failed", "error", err)
			}
		}
	}
}

// migration is a CQL file and/or a Go step (data migrations CQL cannot express). Both must be
// idempotent: a migration interrupted before it is recorded runs again.
type migration struct {
	version    int
	name       string
	statements []string
	run        func(db *DB, ctx context.Context) error
}

// goMigrations are the Go steps, by version.
var goMigrations = []migration{
	{version: 5, name: "0005_rekey_hazard_ids (go)", run: (*DB).rekeyHazardIDs},
	{version: 15, name: "0015_backfill_sharded_projection (go)", run: (*DB).backfillShardedProjection},
}

// loadMigrations returns the NNNN_name.cql files and the Go steps, in version order.
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, err
	}
	var out []migration
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".cql") {
			continue
		}
		prefix, _, _ := strings.Cut(name, "_")
		version, err := strconv.Atoi(prefix)
		if err != nil {
			return nil, fmt.Errorf("migration %s: name must start with a number", name)
		}
		body, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			return nil, err
		}
		out = append(out, migration{version: version, name: name, statements: splitStatements(string(body))})
	}
	out = append(out, goMigrations...)
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	for i := 1; i < len(out); i++ {
		if out[i].version == out[i-1].version {
			return nil, fmt.Errorf("migrations %s and %s share version %d", out[i-1].name, out[i].name, out[i].version)
		}
	}
	return out, nil
}

// splitStatements drops "--" comment lines and splits on ';' at the end of a line.
func splitStatements(body string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		cur.WriteString(line)
		cur.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSuffix(strings.TrimSpace(cur.String()), ";")
			out = append(out, stmt)
			cur.Reset()
		}
	}
	if rest := strings.TrimSpace(cur.String()); rest != "" {
		out = append(out, rest)
	}
	return out
}

func lockHolder() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
}

func validIdentifier(s string) bool {
	for i, r := range s {
		if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return s != "" && len(s) <= 48
}

func defaultString(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

var (
	_ storage.HazardStore      = hazards{}
	_ storage.SourceStateStore = sources{}
	_ storage.Outbox           = (*outbox)(nil)
)
