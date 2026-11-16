CREATE TABLE IF NOT EXISTS m2_stream_journal (
    stream text, block bigint, sequence bigint, kind text, cursor text,
    payload blob, received_at timestamp,
    PRIMARY KEY ((stream, block), sequence)
) WITH CLUSTERING ORDER BY (sequence ASC);
CREATE TABLE IF NOT EXISTS m2_stream_checkpoints (
    stream text PRIMARY KEY, sequence bigint, cursor text
);
CREATE TABLE IF NOT EXISTS m2_parked_inputs (
    source text, message_id text, received_at timestamp, reason text, payload blob,
    PRIMARY KEY ((source), message_id)
) WITH CLUSTERING ORDER BY (message_id ASC);
