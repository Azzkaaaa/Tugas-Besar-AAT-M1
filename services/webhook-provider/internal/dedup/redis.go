package dedup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

const (
	keyPrefix      = "webhook:dedup:"
	inflightPrefix = "inflight:"
	delivered      = "delivered"
)

// Both scripts act only while the key still holds the caller's in-flight token, so a consumer
// whose lease expired cannot overwrite the claim of the consumer that took over.
var (
	markScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("SET", KEYS[1], "delivered", "PX", ARGV[2])
end
return false`)

	releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`)
)

// Redis keeps claims as webhook:dedup:{event_id}:{webhook_id} = "inflight:<token>" (TTL = lease)
// or "delivered" (TTL = the dedup window).
type Redis struct {
	client       *redis.Client
	deliveredTTL time.Duration
}

// NewRedis connects to url (redis://host:port/db) and checks the connection.
func NewRedis(ctx context.Context, url string, deliveredTTL time.Duration) (*Redis, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}
	client := redis.NewClient(opts)
	if err := redisotel.InstrumentTracing(client); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed instrumenting redis: %w", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("unable to reach redis: %w", err)
	}
	return &Redis{client: client, deliveredTTL: deliveredTTL}, nil
}

func key(eventID, webhookID string) string {
	return keyPrefix + eventID + ":" + webhookID
}

func (r *Redis) Claim(ctx context.Context, eventID, webhookID string, lease time.Duration) (Claim, Result, error) {
	claim := newClaim(eventID, webhookID)
	k := key(eventID, webhookID)

	ok, err := r.client.SetNX(ctx, k, inflightPrefix+claim.Token, lease).Result()
	if err != nil {
		return Claim{}, 0, err
	}
	if ok {
		return claim, Claimed, nil
	}

	value, err := r.client.Get(ctx, k).Result()
	switch {
	case errors.Is(err, redis.Nil):
		// Released or expired between the two commands: try once more.
		ok, err = r.client.SetNX(ctx, k, inflightPrefix+claim.Token, lease).Result()
		if err != nil {
			return Claim{}, 0, err
		}
		if ok {
			return claim, Claimed, nil
		}
		return Claim{}, HeldElsewhere, nil
	case err != nil:
		return Claim{}, 0, err
	case value == delivered:
		return Claim{}, AlreadyDelivered, nil
	case strings.HasPrefix(value, inflightPrefix):
		return Claim{}, HeldElsewhere, nil
	default:
		return Claim{}, 0, fmt.Errorf("unexpected dedup value %q at %s", value, k)
	}
}

func (r *Redis) MarkDelivered(ctx context.Context, claim Claim) error {
	err := markScript.Run(ctx, r.client, []string{key(claim.EventID, claim.WebhookID)},
		inflightPrefix+claim.Token, r.deliveredTTL.Milliseconds()).Err()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

func (r *Redis) Release(ctx context.Context, claim Claim) error {
	return releaseScript.Run(ctx, r.client, []string{key(claim.EventID, claim.WebhookID)},
		inflightPrefix+claim.Token).Err()
}

func (r *Redis) Close() error {
	return r.client.Close()
}
