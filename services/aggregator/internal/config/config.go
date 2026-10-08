package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ConsumerQueueConfig defines a single downstream consumer's queue and its DLQ.
type ConsumerQueueConfig struct {
	Name            string
	Queue           string
	DeadLetterQueue string
}

type Config struct {
	AppEnv               string
	LogLevel             string
	GRPCPort             string
	BMKGBaseURL          string
	PVMBGBaseURL         string
	BMKGAPIKey           string
	PVMBGBearerToken     string
	ServiceToken         string
	AuthPublicKeyPEM     string
	AuthPublicKeyPath    string
	BMKGPollInterval     time.Duration
	PVMBGPollInterval    time.Duration
	BMKGRequestTimeout   time.Duration
	PVMBGRequestTimeout  time.Duration
	SourceStaleThreshold time.Duration
	CanonicalStoreDSN    string
	AMQPURL              string
	RabbitMQExchange     string
	// ConsumerQueues is the list of downstream consumer queues the aggregator
	// pre-declares on startup so messages are never silently dropped.
	// Each entry has its own dead-letter queue.
	ConsumerQueues []ConsumerQueueConfig
	// OutboxMaxRetries is the max publish attempts before a record is dead-lettered.
	OutboxMaxRetries int
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

	requireDurationMS := func(key string) time.Duration {
		ms := requireInt(key)
		return time.Duration(ms) * time.Millisecond
	}

	cfg := &Config{
		AppEnv:               requireEnv("APP_ENV"),
		LogLevel:             requireEnv("LOG_LEVEL"),
		GRPCPort:             requireEnv("AGGREGATOR_GRPC_PORT"),
		BMKGBaseURL:          requireEnv("BMKG_BASE_URL"),
		PVMBGBaseURL:         requireEnv("PVMBG_BASE_URL"),
		BMKGAPIKey:           requireEnv("BMKG_API_KEY"),
		PVMBGBearerToken:     requireEnv("PVMBG_BEARER_TOKEN"),
		ServiceToken:         requireEnv("AGGREGATOR_SERVICE_TOKEN"),
		AuthPublicKeyPEM:     os.Getenv("AUTH_PUBLIC_KEY"),
		AuthPublicKeyPath:    os.Getenv("AUTH_PUBLIC_KEY_PATH"),
		BMKGPollInterval:     requireDurationMS("BMKG_POLL_INTERVAL_MS"),
		PVMBGPollInterval:    requireDurationMS("PVMBG_POLL_INTERVAL_MS"),
		BMKGRequestTimeout:   requireDurationMS("BMKG_REQUEST_TIMEOUT_MS"),
		PVMBGRequestTimeout:  requireDurationMS("PVMBG_REQUEST_TIMEOUT_MS"),
		SourceStaleThreshold: requireDurationMS("SOURCE_STALE_AFTER_MS"),
		CanonicalStoreDSN:    requireEnv("CANONICAL_STORE_DSN"),
		AMQPURL:              requireEnv("AGGREGATOR_AMQP_URL"),
		RabbitMQExchange:     requireEnv("RABBITMQ_EXCHANGE"),
		OutboxMaxRetries:     requireInt("OUTBOX_MAX_RETRIES"),
	}

	// Validate auth public key input (must provide either PEM string or path)
	if cfg.AuthPublicKeyPEM == "" && cfg.AuthPublicKeyPath == "" {
		// errs = append(errs, "must set either AUTH_PUBLIC_KEY or AUTH_PUBLIC_KEY_PATH")
		fmt.Printf("[Aggregator] NO AUTH PUBLIC KEY LOADED, USING SERVICE TOKEN ONLY")
	}

	consumerQueues, err := loadConsumerQueues()
	if err != nil {
		errs = append(errs, err.Error())
	} else {
		cfg.ConsumerQueues = consumerQueues
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to load configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}

	return cfg, nil
}

// loadConsumerQueues reads consumer queue config from environment.
// Each consumer is described by three env vars (1-indexed):
//
//	CONSUMER_n_NAME, CONSUMER_n_QUEUE, CONSUMER_n_DLQ
//
// Defaults to the three standard BNPB consumer queues if none are configured.
func loadConsumerQueues() ([]ConsumerQueueConfig, error) {
	var queues []ConsumerQueueConfig

	// Try reading numbered env vars (1-indexed)
	for i := 1; ; i++ {
		prefix := "CONSUMER_" + strconv.Itoa(i) + "_"
		name := os.Getenv(prefix + "NAME")
		queue := os.Getenv(prefix + "QUEUE")
		dlq := os.Getenv(prefix + "DLQ")

		if name == "" && queue == "" && dlq == "" {
			break
		}

		if name == "" || queue == "" || dlq == "" {
			return nil, fmt.Errorf("incomplete consumer queue config at index %d (NAME, QUEUE, and DLQ must all be set)", i)
		}

		queues = append(queues, ConsumerQueueConfig{
			Name:            name,
			Queue:           queue,
			DeadLetterQueue: dlq,
		})
	}

	if len(queues) > 0 {
		fmt.Printf("%d Consumer queues loaded successfully", len(queues))
		return queues, nil
	} else {
		return nil, fmt.Errorf("failed to load consumer queues: no consumer queues configured")
	}
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
