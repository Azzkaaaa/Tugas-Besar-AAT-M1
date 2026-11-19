-- +goose Up
-- Derived caches use immutable publication, bounded rows and transactional
-- retention. Legacy bytea snapshots remain untouched for rolling rollback.
CREATE TABLE m2_stream_snapshot_manifests (
    stream text NOT NULL, sequence bigint NOT NULL, chunks integer NOT NULL,
    bytes bigint NOT NULL, sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (stream,sequence)
);
CREATE TABLE m2_stream_snapshot_parts (
    stream text NOT NULL, sequence bigint NOT NULL, chunk integer NOT NULL,
    data bytea NOT NULL, PRIMARY KEY (stream,sequence,chunk),
    FOREIGN KEY (stream,sequence) REFERENCES m2_stream_snapshot_manifests ON DELETE CASCADE
);

-- +goose Down
DROP TABLE m2_stream_snapshot_parts;
DROP TABLE m2_stream_snapshot_manifests;
