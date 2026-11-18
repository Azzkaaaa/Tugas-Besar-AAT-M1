-- Derived satellite engine state at a committed journal sequence, split into
-- bounded chunks. The index row is written after every chunk and names the
-- digest, so a torn write is detected and skipped. Not fenced: any writer of
-- sequence N writes the same bytes.
CREATE TABLE IF NOT EXISTS m2_stream_snapshot_index (
    stream text, sequence bigint, chunks int, bytes bigint, sha256 text, created_at timestamp,
    PRIMARY KEY ((stream), sequence)
) WITH CLUSTERING ORDER BY (sequence DESC);
CREATE TABLE IF NOT EXISTS m2_stream_snapshot_chunks (
    stream text, sequence bigint, chunk int, data blob,
    PRIMARY KEY ((stream, sequence), chunk)
) WITH CLUSTERING ORDER BY (chunk ASC);
