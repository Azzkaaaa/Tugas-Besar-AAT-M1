-- +goose Up
ALTER TABLE webhook_subscriptions ADD COLUMN fanout_seq BIGINT GENERATED ALWAYS AS IDENTITY;
CREATE UNIQUE INDEX webhook_subscriptions_fanout_seq ON webhook_subscriptions (fanout_seq);
CREATE INDEX webhook_subscriptions_active_fanout ON webhook_subscriptions (fanout_seq) WHERE is_active;

-- +goose Down
DROP INDEX webhook_subscriptions_active_fanout;
DROP INDEX webhook_subscriptions_fanout_seq;
ALTER TABLE webhook_subscriptions DROP COLUMN fanout_seq;
