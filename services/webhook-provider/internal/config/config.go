package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the webhook-provider service.
type Config struct {
	AppEnv             string
	LogLevel           string
	GRPCPort           string
	StoreDSN           string
	AMQPURL            string
	RabbitMQExchange   string
	ConsumerQueue      string
	ConsumerDLQ        string
	ServiceToken       string
	AuthPublicKeyPEM   string
	AuthPublicKeyPath  string
	MaxDeliveryRetries int
	HTTPTimeoutMS      time.Duration
}

// Load reads configuration from environment variables.
// Follows the same pattern as services/aggregator.
func Load() (*Config, error) {
	loadDotEnv()

	var errs []string

	requireEnv := func(key string) string {
		val := os.Getenv(key)
		if strings.TrimSpace(val) == "" {
			errs = append(errs, fmt.Sprintf("missing required env var: %s", key))
		}
		return val
	}

	optionalEnv := func(key, defaultVal string) string {
		val := os.Getenv(key)
		if strings.TrimSpace(val) == "" {
			return defaultVal
		}
		return val
	}

	optionalInt := func(key string, defaultVal int) int {
		val := os.Getenv(key)
		if strings.TrimSpace(val) == "" {
			return defaultVal
		}
		i, err := strconv.Atoi(val)
		if err != nil {
			errs = append(errs, fmt.Sprintf("invalid integer for env var %s: %v", key, err))
			return defaultVal
		}
		return i
	}

	cfg := &Config{
		AppEnv:             requireEnv("APP_ENV"),
		LogLevel:           requireEnv("LOG_LEVEL"),
		GRPCPort:           optionalEnv("WEBHOOK_GRPC_PORT", "9094"),
		StoreDSN:           requireEnv("WEBHOOK_STORE_DSN"),
		AMQPURL:            requireEnv("WEBHOOK_AMQP_URL"),
		RabbitMQExchange:   requireEnv("RABBITMQ_EXCHANGE"),
		ConsumerQueue:      optionalEnv("WEBHOOK_CONSUMER_QUEUE", "bnpb.webhook-provider.v1"),
		ConsumerDLQ:        optionalEnv("WEBHOOK_CONSUMER_DLQ", "bnpb.webhook-provider.dlq.v1"),
		ServiceToken:       requireEnv("WEBHOOK_SERVICE_TOKEN"),
		AuthPublicKeyPEM:   os.Getenv("AUTH_PUBLIC_KEY"),
		AuthPublicKeyPath:  os.Getenv("AUTH_PUBLIC_KEY_PATH"),
		MaxDeliveryRetries: optionalInt("WEBHOOK_MAX_RETRIES", 5),
		HTTPTimeoutMS:      time.Duration(optionalInt("WEBHOOK_HTTP_TIMEOUT_MS", 10000)) * time.Millisecond,
	}

	if cfg.AuthPublicKeyPEM == "" && cfg.AuthPublicKeyPath == "" {
		fmt.Println("[WebhookProvider] WARNING: No AUTH_PUBLIC_KEY configured. Running in unverified token mode (dev only).")
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to load configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}

	return cfg, nil
}

func loadDotEnv() {
	candidates := []string{".env", "../.env", "../../.env"}
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			k := strings.TrimSpace(parts[0])
			v := strings.Trim(strings.TrimSpace(parts[1]), "\"'")
			if os.Getenv(k) == "" && v != "" {
				_ = os.Setenv(k, v)
			}
		}
		break
	}
}
