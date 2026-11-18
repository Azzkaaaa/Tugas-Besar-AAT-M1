-- +goose Up
ALTER TABLE leader_leases ADD COLUMN epoch bigint NOT NULL DEFAULT 0;

-- Check at the actual INSERT/UPDATE statement, including previously unseen
-- hazards. Holding a row share lock serializes takeover with the transaction.
-- +goose StatementBegin
CREATE FUNCTION enforce_leader_authority() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    lease_name text := NULLIF(current_setting('bnpb.lease_name', true), '');
    lease_holder text := current_setting('bnpb.lease_holder', true);
    lease_epoch bigint;
BEGIN
    IF lease_name IS NULL THEN
        RETURN NEW;
    END IF;
    lease_epoch := NULLIF(current_setting('bnpb.lease_epoch', true), '')::bigint;
    PERFORM 1 FROM leader_leases
        WHERE name = lease_name AND holder = lease_holder AND epoch = lease_epoch
          AND expires_at > clock_timestamp()
        FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'stale_leader' USING ERRCODE = 'BN001';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER hazard_write_authority BEFORE INSERT OR UPDATE ON hazard_events
    FOR EACH ROW EXECUTE FUNCTION enforce_leader_authority();
CREATE TRIGGER stream_append_authority BEFORE INSERT OR UPDATE ON m2_stream_journal
    FOR EACH ROW EXECUTE FUNCTION enforce_leader_authority();
CREATE TRIGGER stream_checkpoint_authority BEFORE INSERT OR UPDATE ON m2_stream_checkpoints
    FOR EACH ROW EXECUTE FUNCTION enforce_leader_authority();
CREATE TRIGGER stream_park_authority BEFORE INSERT OR UPDATE ON m2_parked_inputs
    FOR EACH ROW EXECUTE FUNCTION enforce_leader_authority();
CREATE TRIGGER source_state_authority BEFORE INSERT OR UPDATE ON ingest_watermarks
    FOR EACH ROW EXECUTE FUNCTION enforce_leader_authority();

-- +goose Down
-- Preserve the write boundary during application rollback.
