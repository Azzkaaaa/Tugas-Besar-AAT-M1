-- +goose Up
CREATE TABLE webhook_subscriptions (
    webhook_id       TEXT PRIMARY KEY,
    owner_client_id  TEXT NOT NULL,
    url              TEXT NOT NULL,
    secret           TEXT NOT NULL,
    filter_source    INTEGER NOT NULL DEFAULT 0,
    filter_hazard    INTEGER NOT NULL DEFAULT 0,
    filter_severity  INTEGER NOT NULL DEFAULT 0,
    filter_since     TIMESTAMPTZ,
    filter_until     TIMESTAMPTZ,
    view             INTEGER NOT NULL DEFAULT 1,
    description      TEXT NOT NULL DEFAULT '',
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_subscriptions_owner ON webhook_subscriptions (owner_client_id, webhook_id);
CREATE INDEX idx_subscriptions_active ON webhook_subscriptions (webhook_id) WHERE is_active;

CREATE TABLE webhook_deliveries (
    delivery_id   TEXT PRIMARY KEY,
    webhook_id    TEXT NOT NULL REFERENCES webhook_subscriptions (webhook_id) ON DELETE CASCADE,
    event_id      TEXT NOT NULL,
    hazard_id     TEXT NOT NULL,
    status_code   INTEGER NOT NULL DEFAULT 0,
    status        TEXT NOT NULL DEFAULT 'RETRYING',
    attempt       INTEGER NOT NULL DEFAULT 1,
    duration_ms   BIGINT NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    delivered_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_deliveries_webhook ON webhook_deliveries (webhook_id, delivered_at DESC, delivery_id DESC);
CREATE INDEX idx_deliveries_event ON webhook_deliveries (event_id, webhook_id);

-- +goose Down
DROP TABLE webhook_deliveries;
DROP TABLE webhook_subscriptions;
