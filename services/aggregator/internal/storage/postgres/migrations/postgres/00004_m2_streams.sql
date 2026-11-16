-- +goose Up
CREATE TABLE IF NOT EXISTS m2_stream_journal (
    stream text NOT NULL, sequence bigint NOT NULL, kind text NOT NULL,
    cursor text NOT NULL, payload bytea NOT NULL, received_at timestamptz NOT NULL,
    PRIMARY KEY (stream, sequence)
);
CREATE TABLE IF NOT EXISTS m2_stream_checkpoints (
    stream text PRIMARY KEY, sequence bigint NOT NULL, cursor text NOT NULL
);
CREATE TABLE IF NOT EXISTS m2_parked_inputs (
    source text NOT NULL, message_id text NOT NULL, received_at timestamptz NOT NULL,
    reason text NOT NULL, payload bytea NOT NULL, PRIMARY KEY (source, message_id)
);

-- +goose Down
-- Input history is deliberately retained on rollback.
