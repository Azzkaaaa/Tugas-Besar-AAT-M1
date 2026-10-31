package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AppEnv                 string
	LogLevel               string
	GRPCPort               string
	BMKGBaseURL            string
	PVMBGBaseURL           string
	BMKGAPIKey             string
	PVMBGBearerToken       string
	ServiceToken           string
	AuthAuthority          string
	AuthAudience           string
	AuthInsecureSkipVerify bool
	BMKGPollInterval       time.Duration
	PVMBGPollInterval      time.Duration
	BMKGRequestTimeout     time.Duration
	PVMBGRequestTimeout    time.Duration
	SourceStaleThreshold   time.Duration
	CanonicalStoreDSN      string
	AMQPURL                string
	RabbitMQExchange       string
	// OutboxMaxRetries is the max publish attempts before a record is dead-lettered.
	OutboxMaxRetries int
	// OutboxRetention is how long published and dead-lettered records stay in outbox_events.
	OutboxRetention time.Duration
}

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

	requireInt := func(key string) int {
		valStr := requireEnv(key)
		if valStr == "" {
			return 0
		}
		val, err := strconv.Atoi(valStr)
		if err != nil {
			errs = append(errs, fmt.Sprintf("invalid integer for env var %s: %v", key, err))
			return 0
		}
		return val
	}

	optionalInt := func(key string, defaultVal int) int {
		valStr := os.Getenv(key)
		if strings.TrimSpace(valStr) == "" {
			return defaultVal
		}
		val, err := strconv.Atoi(valStr)
		if err != nil || val <= 0 {
			errs = append(errs, fmt.Sprintf("invalid positive integer for env var %s: %q", key, valStr))
			return defaultVal
		}
		return val
	}

	requireDurationMS := func(key string) time.Duration {
		ms := requireInt(key)
		return time.Duration(ms) * time.Millisecond
	}

	insecureSkip := strings.EqualFold(os.Getenv("AUTH_INSECURE_SKIP_VERIFY"), "true")

	cfg := &Config{
		AppEnv:                 requireEnv("APP_ENV"),
		LogLevel:               requireEnv("LOG_LEVEL"),
		GRPCPort:               requireEnv("AGGREGATOR_GRPC_PORT"),
		BMKGBaseURL:            requireEnv("BMKG_BASE_URL"),
		PVMBGBaseURL:           requireEnv("PVMBG_BASE_URL"),
		BMKGAPIKey:             requireEnv("BMKG_API_KEY"),
		PVMBGBearerToken:       requireEnv("PVMBG_BEARER_TOKEN"),
		ServiceToken:           requireEnv("AGGREGATOR_SERVICE_TOKEN"),
		AuthAuthority:          os.Getenv("AUTH_AUTHORITY"),
		AuthAudience:           os.Getenv("AUTH_AUDIENCE"),
		AuthInsecureSkipVerify: insecureSkip,
		BMKGPollInterval:       requireDurationMS("BMKG_POLL_INTERVAL_MS"),
		PVMBGPollInterval:      requireDurationMS("PVMBG_POLL_INTERVAL_MS"),
		BMKGRequestTimeout:     requireDurationMS("BMKG_REQUEST_TIMEOUT_MS"),
		PVMBGRequestTimeout:    requireDurationMS("PVMBG_REQUEST_TIMEOUT_MS"),
		SourceStaleThreshold:   requireDurationMS("SOURCE_STALE_AFTER_MS"),
		CanonicalStoreDSN:      requireEnv("CANONICAL_STORE_DSN"),
		AMQPURL:                requireEnv("AGGREGATOR_AMQP_URL"),
		RabbitMQExchange:       requireEnv("RABBITMQ_EXCHANGE"),
		OutboxMaxRetries:       requireInt("OUTBOX_MAX_RETRIES"),
		OutboxRetention:        time.Duration(optionalInt("OUTBOX_RETENTION_HOURS", 168)) * time.Hour,
	}

	// AUTH_AUTHORITY is required unless insecure mode is explicitly enabled.
	if cfg.AuthAuthority == "" && !insecureSkip {
		errs = append(errs, "AUTH_AUTHORITY is required (or set AUTH_INSECURE_SKIP_VERIFY=true for development)")
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

// func getEnv(key, defaultVal string) string {
// 	if val := os.Getenv(key); val != "" {
// 		return val
// 	}
// 	return defaultVal
// }

// func getEnvInt(key string, defaultVal int) int {
// 	if val := os.Getenv(key); val != "" {
// 		if i, err := strconv.Atoi(val); err == nil {
// 			return i
// 		}
// 	}
// 	return defaultVal
// }
