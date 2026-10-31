// Package config reads the gateway environment (see docs/configuration.md).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                string
	AggregatorAddress   string
	NotificationAddress string
	WebhookAddress      string
	FeedAddress         string
	ResolveInterval     time.Duration
	RequestTimeout      time.Duration
	MaxInFlight         int
	RetryAfterSeconds   int
	Heartbeat           time.Duration
	LogLevel            string
}

func Load() (Config, error) {
	var errs []string
	number := func(name string, fallback int) int {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			return fallback
		}
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			errs = append(errs, name+" must be a positive integer")
			return fallback
		}
		return value
	}
	text := func(name, fallback string) string {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
		return fallback
	}
	c := Config{
		Port:                text("GATEWAY_HTTP_PORT", "8080"),
		AggregatorAddress:   text("AGGREGATOR_GRPC_ADDRESS", "aggregator-service:9091"),
		NotificationAddress: text("NOTIFICATION_GRPC_ADDRESS", "notification-service:9095"),
		WebhookAddress:      text("WEBHOOK_GRPC_ADDRESS", "webhook-provider:9094"),
		FeedAddress:         text("EVENT_FEED_GRPC_ADDRESS", "feed-service:9093"),
		ResolveInterval:     time.Duration(number("GATEWAY_GRPC_RESOLVE_INTERVAL_SECONDS", 30)) * time.Second,
		RequestTimeout:      time.Duration(number("GATEWAY_REQUEST_TIMEOUT_MS", 2000)) * time.Millisecond,
		MaxInFlight:         number("GATEWAY_MAX_IN_FLIGHT", 100),
		RetryAfterSeconds:   number("GATEWAY_RETRY_AFTER_SECONDS", 5),
		Heartbeat:           time.Duration(number("GATEWAY_SSE_HEARTBEAT_SECONDS", 15)) * time.Second,
		LogLevel:            text("LOG_LEVEL", "info"),
	}
	if len(errs) > 0 {
		return c, fmt.Errorf("invalid configuration: %s", strings.Join(errs, "; "))
	}
	return c, nil
}
