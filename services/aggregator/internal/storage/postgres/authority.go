package postgres

import (
	"context"
	"errors"
	"strconv"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (l leases) Fence(ctx context.Context, name, holder string) (leader.Authority, error) {
	token := leader.Authority{Lease: name, Holder: holder}
	err := l.db.Pool.QueryRow(ctx, `SELECT epoch FROM leader_leases WHERE name=$1 AND holder=$2 AND expires_at>clock_timestamp()`, name, holder).Scan(&token.Epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return token, leader.ErrLostAuthority
	}
	return token, err
}
func (l leases) ValidateAuthority(ctx context.Context, a leader.Authority) error {
	var valid bool
	err := l.db.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM leader_leases WHERE name=$1 AND holder=$2 AND epoch=$3 AND expires_at>clock_timestamp())`, a.Lease, a.Holder, a.Epoch).Scan(&valid)
	if err == nil && !valid {
		return leader.ErrLostAuthority
	}
	return err
}

func setWriteAuthority(ctx context.Context, tx pgx.Tx) error {
	a, ok := leader.AuthorityFromContext(ctx)
	if !ok {
		return nil
	}
	_, err := tx.Exec(ctx, `SELECT set_config('bnpb.lease_name',$1,true),set_config('bnpb.lease_holder',$2,true),set_config('bnpb.lease_epoch',$3,true),
		set_config('lock_timeout','2s',true),set_config('statement_timeout','4s',true),set_config('idle_in_transaction_session_timeout','5s',true)`, a.Lease, a.Holder, strconv.FormatInt(a.Epoch, 10))
	return err
}
func authorityError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "BN001" {
		return leader.ErrLostAuthority
	}
	return err
}
func (db *DB) write(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = setWriteAuthority(ctx, tx); err == nil {
		err = fn(tx)
	}
	if err != nil {
		return authorityError(err)
	}
	return authorityError(tx.Commit(ctx))
}

var _ leader.FencingStore = leases{}
