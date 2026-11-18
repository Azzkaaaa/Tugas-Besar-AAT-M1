-- +goose Up
-- Derived satellite engine state at a committed journal sequence. Content is a
-- pure function of the journal prefix, so it is not fenced by write authority.
CREATE TABLE IF NOT EXISTS m2_stream_snapshots (
    stream text NOT NULL, sequence bigint NOT NULL, state bytea NOT NULL,
    sha256 text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, sequence)
);

-- +goose Down
DROP TABLE IF EXISTS m2_stream_snapshots;
