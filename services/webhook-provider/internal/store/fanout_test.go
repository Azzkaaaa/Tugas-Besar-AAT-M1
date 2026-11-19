package store

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func TestSQLiteActiveSubscriptionWalk(t *testing.T) {
	s, err := OpenSQLite(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	testActiveSubscriptionWalk(t, s)
}

func testActiveSubscriptionWalk(t *testing.T, s Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	want := map[string]bool{}
	for i := 0; i < 180; i++ {
		sub := &Subscription{WebhookID: fmt.Sprintf("walk-%03d", i), URL: "https://example.invalid/", Secret: "key", OwnerClientID: "private-owner", IsActive: i%13 != 0, View: 2}
		if i >= 100 {
			sub.Secret = strings.Repeat("é", 12000)
		}
		if i == 178 {
			sub.Secret = strings.Repeat("x", ActiveBatchBytes+1)
		}
		if i == 1 {
			sub.Description = strings.Repeat("private-description", 20000)
		}
		if err := s.CreateSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
		if sub.IsActive {
			want[sub.WebhookID] = true
		}
	}
	head, err := s.ActiveSubscriptionHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The highest row is deleted; a later insert must not reuse its generation.
	if err := s.DeleteSubscription(ctx, "walk-179"); err != nil {
		t.Fatal(err)
	}
	delete(want, "walk-179")
	if err := s.CreateSubscription(ctx, &Subscription{WebhookID: "000-late", URL: "http://late.invalid", Secret: "key", IsActive: true}); err != nil {
		t.Fatal(err)
	}
	newHead, err := s.ActiveSubscriptionHead(ctx)
	if err != nil || newHead <= head {
		t.Fatalf("generation reused: %d <= %d: %v", newHead, head, err)
	}
	seen, pages, oversized := map[string]bool{}, 0, false
	for after := int64(0); after < head; {
		batch, next, err := s.ListActiveSubscriptions(ctx, after, head)
		if err != nil {
			t.Fatal(err)
		}
		if next <= after || next > head || len(batch) > ActiveBatchRows {
			t.Fatalf("invalid bounded batch: after=%d next=%d head=%d size=%d", after, next, head, len(batch))
		}
		if pages == 0 && len(batch) != ActiveBatchRows {
			t.Fatalf("row limit was not exercised: %d", len(batch))
		}
		bytes := 0
		for _, sub := range batch {
			if !want[sub.WebhookID] || seen[sub.WebhookID] {
				t.Fatalf("unexpected/duplicate subscription %s", sub.WebhookID)
			}
			if sub.Description != "" || sub.OwnerClientID != "" {
				t.Fatal("fanout hydrated private management fields")
			}
			seen[sub.WebhookID] = true
			bytes += len(sub.URL) + len(sub.Secret)
		}
		if bytes > ActiveBatchBytes {
			if len(batch) != 1 || batch[0].WebhookID != "walk-178" {
				t.Fatalf("batch exceeds byte limit: %d rows, %d bytes", len(batch), bytes)
			}
			oversized = true // Boundary rows stay intact rather than disappearing.
		}
		if len(batch) > 0 {
			// Also proves SQLite did not leave its only connection's reader open.
			if err := s.InsertDelivery(ctx, &Delivery{DeliveryID: fmt.Sprintf("walk-log-%d", pages), WebhookID: batch[0].WebhookID, EventID: "e", HazardID: "h", Status: "SUCCESS", DeliveredAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
		}
		after = next
		pages++
	}
	if len(seen) != len(want) || pages < 6 || !oversized {
		t.Fatalf("incomplete walk: got=%d want=%d pages=%d oversized=%v", len(seen), len(want), pages, oversized)
	}
	if err := s.DeleteSubscription(ctx, "000-late"); err != nil {
		t.Fatal(err)
	}
	batch, next, err := s.ListActiveSubscriptions(ctx, head, newHead)
	if err != nil || len(batch) != 0 || next != newHead {
		t.Fatalf("deleted tail did not advance: %v %d %v", batch, next, err)
	}
}

func TestSQLiteFanoutLegacyUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE webhook_subscriptions (
		webhook_id TEXT PRIMARY KEY, owner_client_id TEXT NOT NULL, url TEXT NOT NULL, secret TEXT NOT NULL,
		filter_source INTEGER DEFAULT 0, filter_hazard INTEGER DEFAULT 0, filter_severity INTEGER DEFAULT 0,
		filter_since DATETIME, filter_until DATETIME, view INTEGER DEFAULT 1, description TEXT DEFAULT '',
		is_active INTEGER DEFAULT 1, created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL);
		INSERT INTO webhook_subscriptions(webhook_id,owner_client_id,url,secret,created_at,updated_at)
		VALUES('legacy','owner','http://example.invalid','secret','2026-11-19T00:00:00.000000000Z','2026-11-19T00:00:00.000000000Z');`)
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.ActiveSubscriptionHead(ctx)
	if err != nil || head != 1 {
		t.Fatalf("legacy head=%d: %v", head, err)
	}
	s.Close()
	s, err = OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	batch, next, err := s.ListActiveSubscriptions(ctx, 0, head)
	if err != nil || len(batch) != 1 || next != head || batch[0].WebhookID != "legacy" {
		t.Fatalf("upgrade changed legacy row: %v %d %v", batch, next, err)
	}
	if err := s.CreateSubscription(ctx, &Subscription{WebhookID: "new", URL: "http://example.invalid", Secret: "s", IsActive: true}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ActiveSubscriptionHead(ctx); err != nil || got != 2 {
		t.Fatalf("reopen generation=%d: %v", got, err)
	}
}

// Optional real-provider test. It owns only a new schema in the named fixture DB.
func TestPostgresActiveSubscriptionWalkAndUpgrade(t *testing.T) {
	dsn := os.Getenv("WEBHOOK_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("WEBHOOK_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database != "webhook_m2_fixture" {
		t.Fatal("refusing non-fixture database")
	}
	admin, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("fanout_test_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	dsn = u.String()
	cfg, err = pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	migrations, err := fs.Sub(postgresMigrations, "migrations/postgres")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = provider.UpTo(ctx, 1); err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO webhook_subscriptions(webhook_id,owner_client_id,url,secret,created_at,updated_at) VALUES('legacy','owner','http://example.invalid','secret',now(),now())`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	s, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	head, err := s.ActiveSubscriptionHead(ctx)
	batch, next, readErr := s.ListActiveSubscriptions(ctx, 0, head)
	if err != nil || readErr != nil || head != 1 || next != head || len(batch) != 1 || batch[0].WebhookID != "legacy" {
		t.Fatalf("legacy migration: head=%d batch=%v next=%d errors=%v/%v", head, batch, next, err, readErr)
	}
	if err := s.DeleteSubscription(ctx, "legacy"); err != nil {
		t.Fatal(err)
	}
	testActiveSubscriptionWalk(t, s)
}
