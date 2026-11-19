-- Immutable index publication and digest-addressed chunks prevent a second
-- writer/codec for the same sequence from corrupting the first saved snapshot.
-- A partial upload has no index and is never a recovery candidate.
CREATE TABLE IF NOT EXISTS m2_stream_snapshot_manifests (
    stream text, sequence bigint, chunks int, bytes bigint, sha256 text,
    created_at timestamp, PRIMARY KEY ((stream),sequence)
) WITH CLUSTERING ORDER BY (sequence DESC);
CREATE TABLE IF NOT EXISTS m2_stream_snapshot_parts (
    stream text, sequence bigint, sha256 text, chunk int, data blob,
    PRIMARY KEY ((stream,sequence),sha256,chunk)
) WITH CLUSTERING ORDER BY (sha256 ASC,chunk ASC);
