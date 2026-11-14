package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/rekey"
	"github.com/pressly/goose/v3"
)

// rekeyMigration (version 3) moves hazards from random IDs to domain.HazardID, in one
// transaction: hazard_events, the hazard_id of outbox events and dead letters, and the payload of
// events not yet published (published history keeps its payload). Revisions are kept. Rows that
// already have their derived ID are skipped, so it is a no-op on new databases. One-way: the old
// random IDs are not kept.
var rekeyMigration = goose.NewGoMigration(3, &goose.GoFunc{RunTx: rekeyHazardIDs}, nil)

func rekeyHazardIDs(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT hazard_id, source, source_ref_id FROM hazard_events FOR UPDATE`)
	if err != nil {
		return err
	}
	type move struct{ from, to string }
	var moves []move
	for rows.Next() {
		var id, source, ref string
		if err := rows.Scan(&id, &source, &ref); err != nil {
			rows.Close()
			return err
		}
		if derived := domain.HazardID(source, ref); derived != id {
			moves = append(moves, move{id, derived})
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, m := range moves {
		for _, stmt := range []string{
			`UPDATE hazard_events SET hazard_id = $2 WHERE hazard_id = $1`,
			`UPDATE outbox_events SET hazard_id = $2 WHERE hazard_id = $1`,
			`UPDATE outbox_dead_letters SET hazard_id = $2 WHERE hazard_id = $1`,
		} {
			if _, err := tx.ExecContext(ctx, stmt, m.from, m.to); err != nil {
				return fmt.Errorf("re-key %s: %w", m.from, err)
			}
		}
	}

	for _, q := range []struct{ query, update string }{
		{`SELECT event_id, hazard_id, payload FROM outbox_events WHERE published_at IS NULL AND failed_at IS NULL`,
			`UPDATE outbox_events SET payload = $2 WHERE event_id = $1`},
		{`SELECT event_id, hazard_id, payload FROM outbox_dead_letters`,
			`UPDATE outbox_dead_letters SET payload = $2 WHERE event_id = $1`},
	} {
		if err := rewritePayloads(ctx, tx, q.query, q.update); err != nil {
			return err
		}
	}
	return nil
}

// rewritePayloads sets payload.hazard_id to each row's hazard_id where it differs.
func rewritePayloads(ctx context.Context, tx *sql.Tx, query, update string) error {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	type change struct {
		eventID string
		payload []byte
	}
	var changes []change
	for rows.Next() {
		var eventID, hazardID string
		var payload []byte
		if err := rows.Scan(&eventID, &hazardID, &payload); err != nil {
			rows.Close()
			return err
		}
		out, err := rekey.Payload(payload, hazardID)
		if err != nil {
			rows.Close()
			return fmt.Errorf("outbox event %s: %w", eventID, err)
		}
		if string(out) != string(payload) {
			changes = append(changes, change{eventID, out})
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, c := range changes {
		if _, err := tx.ExecContext(ctx, update, c.eventID, c.payload); err != nil {
			return err
		}
	}
	return nil
}
