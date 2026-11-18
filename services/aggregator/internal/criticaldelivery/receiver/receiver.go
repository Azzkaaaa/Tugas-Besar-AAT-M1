// Package receiver implements the Field Team's durable notification client.
// A successful response proves a committed local receipt, not provider acceptance.
package receiver

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	_ "modernc.org/sqlite"
)

var ErrConflict = errors.New("event identity already has a different payload")
var ErrInvalid = errors.New("not a critical coastal notification")

type Receiver struct {
	db         *sql.DB
	key        ed25519.PrivateKey
	ID, Secret string
	Now        func() time.Time
	capacity   chan struct{}
}

func Open(path, id, secret string, key ed25519.PrivateKey) (*Receiver, error) {
	if len(key) != ed25519.PrivateKeySize || !bytes.Equal(key, ed25519.NewKeyFromSeed(key[:ed25519.SeedSize])) || len(secret) < 16 || id == "" {
		return nil, errors.New("invalid receiver identity/key/secret")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{`PRAGMA journal_mode=WAL`, `PRAGMA synchronous=FULL`, `PRAGMA busy_timeout=5000`, `PRAGMA cache_size=-8192`,
		`CREATE TABLE IF NOT EXISTS receiver_identity (id INTEGER PRIMARY KEY CHECK(id=1),receiver_id TEXT NOT NULL,public_key TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS receipts (event_id TEXT PRIMARY KEY,hazard_id TEXT NOT NULL,payload_sha256 TEXT NOT NULL,payload BLOB NOT NULL,receipt BLOB NOT NULL,received_at TEXT NOT NULL)`} {
		if _, err = db.Exec(statement); err != nil {
			db.Close()
			return nil, err
		}
	}
	public := base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey))
	if _, err = db.Exec(`INSERT INTO receiver_identity (id,receiver_id,public_key) VALUES (1,?,?) ON CONFLICT DO NOTHING`, id, public); err != nil {
		db.Close()
		return nil, err
	}
	var storedID, storedKey string
	if err = db.QueryRow(`SELECT receiver_id,public_key FROM receiver_identity WHERE id=1`).Scan(&storedID, &storedKey); err != nil {
		db.Close()
		return nil, err
	}
	if storedID != id || storedKey != public {
		db.Close()
		return nil, errors.New("receiver database belongs to a different signing identity")
	}
	return &Receiver{db: db, key: key, ID: id, Secret: secret, Now: time.Now, capacity: make(chan struct{}, 8)}, nil
}
func (r *Receiver) Close() error { return r.db.Close() }

func (r *Receiver) Receive(ctx context.Context, body []byte) (storage.CriticalReceipt, bool, error) {
	var message struct {
		EventID string              `json:"event_id"`
		Data    domain.HazardRecord `json:"data"`
	}
	if len(body) > criticaldelivery.MaxPayload || json.Unmarshal(body, &message) != nil {
		return storage.CriticalReceipt{}, false, ErrInvalid
	}
	critical, _ := message.Data.Attributes["critical"].(bool)
	if !critical || message.Data.Source != domain.SourceCoastal || message.Data.HazardType != domain.HazardTypeCoastal || message.Data.Revision < 1 || message.EventID != domain.EventID(message.Data.HazardID, message.Data.Revision) || domain.Withdrawn(&message.Data) {
		return storage.CriticalReceipt{}, false, ErrInvalid
	}
	hash := sha256.Sum256(body)
	receipt, err := criticaldelivery.SignReceipt(r.key, storage.CriticalReceipt{EventID: message.EventID, HazardID: message.Data.HazardID, ReceiverID: r.ID, PayloadSHA256: hex.EncodeToString(hash[:]), ReceivedAt: r.Now().UTC()})
	if err != nil {
		return receipt, false, err
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return receipt, false, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return receipt, false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO receipts (event_id,hazard_id,payload_sha256,payload,receipt,received_at) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING`, receipt.EventID, receipt.HazardID, receipt.PayloadSHA256, body, encoded, receipt.ReceivedAt.Format(time.RFC3339Nano))
	if err != nil {
		return receipt, false, err
	}
	inserted, _ := result.RowsAffected()
	var storedHash string
	var storedReceipt []byte
	if err = tx.QueryRowContext(ctx, `SELECT payload_sha256,receipt FROM receipts WHERE event_id=?`, receipt.EventID).Scan(&storedHash, &storedReceipt); err != nil {
		return receipt, false, err
	}
	if storedHash != receipt.PayloadSHA256 {
		return receipt, false, ErrConflict
	}
	if err = json.Unmarshal(storedReceipt, &receipt); err != nil {
		return receipt, false, err
	}
	// synchronous=FULL commits WAL and receipt before HTTP can return success.
	if err = tx.Commit(); err != nil {
		return receipt, false, err
	}
	return receipt, inserted == 1, nil
}

func (r *Receiver) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if q.Method == http.MethodGet && q.URL.Path == "/healthz" {
			w.WriteHeader(204)
			return
		}
		if q.Method != http.MethodPost || q.URL.Path != "/notifications" {
			http.NotFound(w, q)
			return
		}
		select {
		case r.capacity <- struct{}{}:
			defer func() { <-r.capacity }()
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "receiver busy", 503)
			return
		}
		q.Body = http.MaxBytesReader(w, q.Body, criticaldelivery.MaxPayload)
		body, err := io.ReadAll(q.Body)
		if err != nil {
			http.Error(w, "notification too large/incomplete", 413)
			return
		}
		if !criticaldelivery.ValidRequestMAC(r.Secret, body, q.Header.Get("X-BNPB-Signature")) {
			http.Error(w, "invalid sender signature", 401)
			return
		}
		receipt, inserted, err := r.Receive(q.Context(), body)
		if errors.Is(err, ErrConflict) {
			http.Error(w, "event payload mismatch", 409)
			return
		}
		if errors.Is(err, ErrInvalid) {
			http.Error(w, "invalid critical notification", 400)
			return
		}
		if err != nil {
			http.Error(w, "receipt not durable; retry", 503)
			return
		}
		slog.InfoContext(q.Context(), "Field Team received critical notification", "event_id", receipt.EventID, "correlation_id", receipt.EventID, "receiver_id", r.ID, "duplicate", !inserted)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(receipt)
	})
}

func (r *Receiver) Export(ctx context.Context, out io.Writer) error {
	rows, err := r.db.QueryContext(ctx, `SELECT receipt FROM receipts ORDER BY event_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		if _, err := out.Write(append(raw, '\n')); err != nil {
			return err
		}
	}
	return rows.Err()
}
